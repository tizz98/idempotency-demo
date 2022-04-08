import structlog
from fastapi import FastAPI

from mars.db import engine, get_session_maker
from mars.models import BaseDAO, IdempotencyKeyDAO, UserDAO  # noqa: F401
from mars.routers import files

logger = structlog.get_logger(__name__)
sessionmaker = get_session_maker()


app = FastAPI()
app.include_router(files.router)


@app.on_event("startup")
async def startup():
    import mars.utils.logging_utils  # noqa: F401

    logger.info("Starting up")

    async with engine.begin() as conn:
        await conn.run_sync(BaseDAO.metadata.drop_all)
        await conn.run_sync(BaseDAO.metadata.create_all)

    async with sessionmaker() as session:
        user = UserDAO(email="eli@noteable.io")
        session.add(user)
        await session.commit()
