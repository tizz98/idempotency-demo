from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


engine = create_async_engine(
    "postgresql+asyncpg://mars:mars@localhost:5432/mars",
    isolation_level="SERIALIZABLE",
    echo=True,
)


def get_session_maker():
    """Call to get an `AsyncSession`."""

    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
