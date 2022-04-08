from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(
    "postgresql+asyncpg://mars:mars@localhost:5432/mars",
    isolation_level="SERIALIZABLE",
    echo=False,
)
read_committed_engine = engine.execution_options(isolation_level="READ COMMITTED")

session_maker = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
read_committed_session_maker = sessionmaker(read_committed_engine, expire_on_commit=False, class_=AsyncSession)


def get_session_maker():
    """Call to get an `AsyncSession`."""

    return sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
