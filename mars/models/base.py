from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import as_declarative


class PaginationParams:
    __slots__ = ("limit", "offset")

    def __init__(self, limit: int = 20, offset: int = 0):
        self.limit = limit
        self.offset = offset


@as_declarative()
class BaseDAO:
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    created_at = sa.Column(
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
        default=sa.func.now(),
    )
    updated_at = sa.Column(
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
        default=sa.func.now(),
        onupdate=sa.func.now(),
    )

    @classmethod
    async def get_by_id(cls, session: AsyncSession, model_id: int) -> Optional["BaseDAO"]:
        stmt = select(cls).where(cls.id == model_id)
        return await (await session.execute(stmt)).scalar_one()

    @classmethod
    async def get_all(cls, session: AsyncSession, pagination_params: PaginationParams) -> list["BaseDAO"]:
        stmt = select(cls).limit(pagination_params.limit).offset(pagination_params.offset)
        return await (await session.execute(stmt)).scalars()

    @classmethod
    async def create(cls, session: AsyncSession, **kwargs) -> "BaseDAO":
        model = cls(**kwargs)
        session.add(model)
        return model

    def update(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    async def delete(self, session: AsyncSession) -> None:
        await session.delete(self)


class IdempotentBaseDAO(BaseDAO):
    __abstract__ = True

    idempotency_key_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("idempotency_keys.id", ondelete="SET NULL"),
    )

    @classmethod
    async def get_by_idempotency_key(
        cls, session: AsyncSession, idempotency_key_id: int
    ) -> Optional["IdempotentBaseDAO"]:
        stmt = select(cls).where(cls.idempotency_key_id == idempotency_key_id)
        return (await session.execute(stmt)).scalar()
