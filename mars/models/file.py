import os.path
import pathlib
import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from pydantic import BaseModel, validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .base import BaseDAO


class CreateFile(BaseModel):
    name: str
    content_base64: str

    @validator("name")
    def validate_name(cls, v):
        if pathlib.Path(v).is_absolute():
            raise ValueError("File name must be relative")
        # TODO: other safety checks
        return v

    def get_s3_key(self) -> str:
        return os.path.join("bucket", uuid.uuid4().hex, self.name)


class File(BaseModel):
    id: int
    name: str
    created_at: datetime
    updated_at: datetime
    upload_completed_at: Optional[datetime]

    class Config:
        orm_mode = True


class FileDAO(BaseDAO):
    __tablename__ = "files"
    __table_args__ = (sa.UniqueConstraint("user_id", "idempotency_key_id"),)

    idempotency_key_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("idempotency_keys.id", ondelete="SET NULL"),
    )

    user_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )

    name = sa.Column(sa.Text, nullable=False)

    s3_key = sa.Column(sa.Text, nullable=False)
    upload_completed_at = sa.Column(sa.TIMESTAMP(timezone=True))

    @classmethod
    async def get_by_idempotency_key(cls, session: AsyncSession, idempotency_key_id: int) -> Optional["FileDAO"]:
        stmt = select(cls).where(cls.idempotency_key_id == idempotency_key_id)
        return (await session.execute(stmt)).scalar()
