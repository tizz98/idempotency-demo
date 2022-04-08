from typing import Optional

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from mars.models.base import BaseDAO


class IdempotencyKeyRecoveryPoint:
    STARTED = "started"
    FINISHED = "finished"


class IdempotencyKeyDAO(BaseDAO):
    __tablename__ = "idempotency_keys"
    __table_args__ = (sa.UniqueConstraint("user_id", "idempotency_key"),)

    idempotency_key = sa.Column(sa.String(length=100), nullable=False)
    last_run_at = sa.Column(
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
        default=sa.func.now(),
    )
    locked_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), default=sa.func.now())

    # parameters of the incoming request
    request_method = sa.Column(sa.String(length=10), nullable=False)
    request_params = sa.Column(JSONB, nullable=False)
    request_path = sa.Column(sa.String(length=100), nullable=False)

    # for finished requests, stored status code and response body
    response_code = sa.Column(sa.Integer)
    response_body = sa.Column(JSONB)

    recovery_point = sa.Column(sa.String(length=50), nullable=False)

    user_id = sa.Column(sa.BigInteger, sa.ForeignKey("users.id"), nullable=False)
    user = orm.relationship("UserDAO")

    @classmethod
    async def get_by_key(cls, session: AsyncSession, /, key: str, user_id: int) -> "IdempotencyKeyDAO":
        key = await cls.get_by_key_or_none(session, key, user_id)
        if key is None:
            raise NoResultFound("Idempotency key not found")
        return key

    @classmethod
    async def get_by_key_or_none(
        cls, session: AsyncSession, /, key: str, user_id: int
    ) -> Optional["IdempotencyKeyDAO"]:
        stmt = select(cls).where(cls.user_id == user_id, cls.idempotency_key == key)
        return (await session.execute(stmt)).scalar()

    @classmethod
    async def update_by_key(cls, session: AsyncSession, /, key: str, user_id: int, **kwargs) -> None:
        stmt = update(cls).where(cls.user_id == user_id, cls.idempotency_key == key).values(**kwargs)
        await session.execute(stmt)
