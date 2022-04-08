import time
from typing import Optional, Union

import structlog
from sqlalchemy import event
from sqlalchemy.engine import Engine

# region General logging

# endregion


# region SQLAlchemy logging


def _begin_statement_log(conn):
    # Stash TX start time.
    conn.info.setdefault("query_start_time", []).append(time.monotonic())

    logger = structlog.get_logger("sqlalchemy.engine.Engine.execute")
    logger.info("BEGIN")


def _tx_end_log_factory(message: str):
    def log_event(conn):
        # See begin_statement_log()
        total = time.monotonic() - conn.info["query_start_time"].pop(-1)

        logger = structlog.get_logger("sqlalchemy.engine.Engine.execute")
        logger.info(message, total_transaction_time=total)

    return log_event


# Reference for all events: https://docs.sqlalchemy.org/en/14/core/events.html
# Recipe for profiling: https://github.com/sqlalchemy/sqlalchemy/wiki/Profiling
event.listen(Engine, "begin", _begin_statement_log)
event.listen(Engine, "commit", _tx_end_log_factory("COMMIT"))
event.listen(Engine, "rollback", _tx_end_log_factory("ROLLBACK"))


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.monotonic())


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(
    conn,
    cursor,
    statement: str,
    parameters: Optional[Union[list, dict, tuple]],
    context,
    executemany: bool,
):
    """Logs query
    * Using the total query time, not the query end time
    """
    total = time.monotonic() - conn.info["query_start_time"].pop(-1)
    logger = structlog.get_logger("sqlalchemy.engine.Engine.execute")

    # This can be set to debug later if we don't want to spam all the SQL queries down the road
    logger.info(
        f"Query: {statement}",
        query_time=total,
        query_time_ms=total * 1000,
        parameters=parameters,
    )


# endregion
