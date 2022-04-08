import sqlalchemy as sa

from mars.models.base import BaseDAO


class UserDAO(BaseDAO):
    __tablename__ = "users"

    email = sa.Column(sa.String(length=255), nullable=False, unique=True)
