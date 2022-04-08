from mars.models.base import BaseDAO
import sqlalchemy as sa


class UserDAO(BaseDAO):
    __tablename__ = "users"

    email = sa.Column(sa.String(length=255), nullable=False, unique=True)
