import os.path
import pathlib
import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from pydantic import BaseModel, validator

from .base import IdempotentBaseDAO


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
        return os.path.join(uuid.uuid4().hex, self.name)


class UpdateFile(BaseModel):
    name: Optional[str]
    content_base64: Optional[str]

    @validator("name")
    def validate_name(cls, v):
        if pathlib.Path(v).is_absolute():
            raise ValueError("File name must be relative")
        # TODO: other safety checks
        return v

    def get_s3_key(self) -> str:
        assert self.name is not None, "name must be set"
        return os.path.join(uuid.uuid4().hex, self.name)


class File(BaseModel):
    id: int
    name: str
    created_at: datetime
    updated_at: datetime
    upload_completed_at: Optional[datetime]

    class Config:
        orm_mode = True


class FileDAO(IdempotentBaseDAO):
    __tablename__ = "files"
    __table_args__ = (sa.UniqueConstraint("user_id", "idempotency_key_id"),)

    user_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )

    name = sa.Column(sa.Text, nullable=False)

    s3_key = sa.Column(sa.Text, nullable=False)
    upload_completed_at = sa.Column(sa.TIMESTAMP(timezone=True))
