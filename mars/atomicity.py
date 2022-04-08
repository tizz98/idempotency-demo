import abc
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

import orjson
import structlog
from asyncpg import SerializationError
from fastapi import Depends, Header, HTTPException, Request
from fastapi.responses import ORJSONResponse
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction
from starlette import status

from mars.db import session_maker
from mars.models.idempotency_key import IdempotencyKeyDAO, IdempotencyKeyRecoveryPoint

logger = structlog.get_logger(__name__)


class ReturnValue(abc.ABC):
    @abc.abstractmethod
    async def call(self, key: IdempotencyKeyDAO) -> None:
        """Modify `key` in some way."""


class RecoveryPoint(ReturnValue):
    def __init__(self, name: str):
        self.name = name

    async def call(self, key: IdempotencyKeyDAO) -> None:
        key.update(recovery_point=self.name)


class Response(ReturnValue):
    def __init__(self, status_code: int, data: Any):
        self.status_code = status_code
        self.data = data

    async def call(self, key: IdempotencyKeyDAO) -> None:
        key.update(
            locked_at=None,
            recovery_point=IdempotencyKeyRecoveryPoint.FINISHED,
            response_code=self.status_code,
            response_body=self.data,
        )


class AtomicPhase:
    UNSET = object()

    # Number of seconds passed which we consider a held idempotency key lock to be
    # defunct and eligible to be locked again by a different API call. We try to
    # unlock keys on our various failure conditions, but software is buggy, and
    # this might not happen 100% of the time, so this is a hedge against it.
    LOCK_TIMEOUT = timedelta(seconds=90)

    def __init__(self, idempotency_key: str, user_id: int):
        super().__init__()

        self.idempotency_key = idempotency_key
        self.user_id = user_id
        self.session: Union[AsyncSession, AtomicPhase.UNSET] = AtomicPhase.UNSET
        self._transaction: Union[AsyncSessionTransaction, AtomicPhase.UNSET] = AtomicPhase.UNSET
        self._return_value: Union[ReturnValue, AtomicPhase.UNSET] = AtomicPhase.UNSET
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self):
        self.session = session_maker()
        await self._exit_stack.enter_async_context(self.session)
        self._transaction = await self._exit_stack.enter_async_context(self.session.begin())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

        if exc_type is not None:
            try:
                async with session_maker() as session, session.begin():
                    await IdempotencyKeyDAO.update_by_key(session, self.idempotency_key, self.user_id, locked_at=None)
            except:  # noqa
                logger.critical("failed to update idempotency key", exc_info=True)

            if isinstance(exc_val, SerializationError):
                raise HTTPException(status.HTTP_409_CONFLICT, detail="retry")

            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="internal server error",
            )

        if self._return_value is AtomicPhase.UNSET:
            return

        async with AtomicPhase(self.idempotency_key, self.user_id) as return_phase:
            key = await IdempotencyKeyDAO.get_by_key(return_phase.session, self.idempotency_key, self.user_id)
            await self._return_value.call(key)

    def set_recovery_point(self, name: str):
        if self._return_value is not AtomicPhase.UNSET:
            raise RuntimeError("recovery point already set")
        self._return_value = RecoveryPoint(name)

    def set_response(self, status_code: int, data: Any):
        if self._return_value is not AtomicPhase.UNSET:
            raise RuntimeError("response already set")
        self._return_value = Response(status_code, data)


class AtomicPhaseGroup:
    def __init__(self, idempotency_key: str, user_id: int, request: Request):
        self.idempotency_key = idempotency_key
        self.user_id = user_id
        self.request = request

    async def __aenter__(self):
        await self.start_atomic_phases()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    def atomic_phase(self) -> AtomicPhase:
        return AtomicPhase(self.idempotency_key, self.user_id)

    async def get_idempotency_key(self) -> Optional[IdempotencyKeyDAO]:
        async with session_maker() as session:
            return await IdempotencyKeyDAO.get_by_key_or_none(session, self.idempotency_key, self.user_id)

    async def get_response(self) -> ORJSONResponse:
        key = await self.get_idempotency_key()
        assert key is not None, "idempotency key not found"
        assert (
            key.recovery_point == IdempotencyKeyRecoveryPoint.FINISHED
        ), "idempotency key recovery point is not finished"
        return ORJSONResponse(key.response_body, key.response_code)

    async def start_atomic_phases(self) -> None:
        async with self.atomic_phase() as phase:
            key = await self.get_idempotency_key()
            request_body = await self.request.body()
            request_body_dict = orjson.loads(request_body)

            if key:
                # programs sending multiple requests with the same idempotency key
                # but different body should be rejected
                if key.request_params != request_body_dict:
                    raise HTTPException(status.HTTP_409_CONFLICT, "param mismatch")

                # only acquire a lock if the key is unlocked or its lock has expired
                # because the original request was long enough ago
                if key.locked_at and key.locked_at > datetime.now(timezone.utc) - AtomicPhase.LOCK_TIMEOUT:
                    raise HTTPException(status.HTTP_409_CONFLICT, "request in progress")

                # Lock the key and update the latest run unless the request is already finished
                if key.recovery_point != IdempotencyKeyRecoveryPoint.FINISHED:
                    key.update(
                        last_run_at=datetime.now(timezone.utc),
                        locked_at=datetime.now(timezone.utc),
                    )
            else:
                await IdempotencyKeyDAO.create(
                    phase.session,
                    idempotency_key=self.idempotency_key,
                    user_id=self.user_id,
                    locked_at=datetime.now(timezone.utc),
                    recovery_point=IdempotencyKeyRecoveryPoint.STARTED,
                    request_method=self.request.method,
                    request_params=request_body_dict,
                    request_path=self.request.url.path,
                    request_path_params=self.request.path_params,
                )

    @classmethod
    async def dependency(
        cls,
        request: Request,
        x_idempotency_key: str = Header(""),
        user_id: int = Depends(lambda: 1),
    ):
        """Create a `AtomicPhaseGroup` via FastAPI dependency injection."""

        if not x_idempotency_key:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "missing X-Idempotency-Key header")

        async with cls(
            idempotency_key=x_idempotency_key,
            user_id=user_id,
            request=request,
        ) as atomic_phase_group:
            yield atomic_phase_group
