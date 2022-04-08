from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import as_declarative


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
    async def create(cls, session: AsyncSession, **kwargs) -> "BaseDAO":
        model = cls(**kwargs)
        session.add(model)
        return model

    def update(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
