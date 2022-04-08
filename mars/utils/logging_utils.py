import time
from datetime import datetime
from typing import Any, Optional, Union

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
    * Using the total query time, not the query end time,
    * Having redacted potentially PII-laden string portions of 'parameters' if is an insert/update statement.
    """
    total = time.monotonic() - conn.info["query_start_time"].pop(-1)

    if context.isinsert or context.isupdate:
        parameters = censor_before_logging(parameters)

    logger = structlog.get_logger("sqlalchemy.engine.Engine.execute")

    # This can be set to debug later if we don't want to spam all the SQL queries down the road
    logger.info(
        f"Query: {statement}",
        query_time=total,
        query_time_ms=total * 1000,
        parameters=parameters,
    )


# strings corresponding to these dict keys being SQLA insert/update logged are informative
# for chasing down bugs and won't ever hold PII.
# 'execution_state', 'kernel_id_1' are deep in multi_kernel_manager query(ies)
UNCENSORED_DICT_KEYS = {"execution_state", "kernel_id_1"}


def censor_before_logging(parameters: Any) -> Any:
    """Censor INSERT or UPDATE statement parameters so that potentially toxic PII doesn't end up in DataDog.

    Does not mutate incoming parameters (if is a container type)

    Censors most strings recursively contained, as the string *might* contain PII.

    If parameters is a container type, will return a (possibly mutated) deep copy so as to not mutate
    the original container the caller made the SQLA .execute() or .executemany() call with.
    """

    # Standard recursion design.

    # Handle scalars, including None
    if not (isinstance(parameters, dict) or isinstance(parameters, list) or isinstance(parameters, tuple)):
        # Ground condition: have some sort of scalar here.
        # Squish it if it is a string; might possibly contain toxic PII from end-users.
        return parameters if not isinstance(parameters, str) else "*CENSORED*"

    else:
        if isinstance(parameters, dict):
            #  We go recursive here -- might have JSONB columns!
            return {k: censor_before_logging(v) if k not in UNCENSORED_DICT_KEYS else v for k, v in parameters.items()}
        else:
            # gots us a list or a tuple. Main case here is when executemany() is called with a list of dicts,
            # but could possibly be old-style query called with only positional params. Recurse.
            return [censor_before_logging(elem) for elem in parameters]


def format_dict_for_log_params(d: Any, parent: str) -> dict:
    """Returns a valid dictionary of log parameters."""
    result = {}

    if isinstance(d, dict):
        for key, value in d.items():
            if isinstance(value, dict):
                result.update(format_dict_for_log_params(value, f"{parent}.{key}"))
            elif isinstance(value, list):
                for i, list_value in enumerate(value):
                    result.update(format_dict_for_log_params(list_value, f"{parent}.{key}[{i}]"))
            else:
                result.update(format_dict_for_log_params(value, f"{parent}.{key}"))
    elif isinstance(d, datetime):
        result[parent] = d.isoformat()
    else:
        result[parent] = d

    return result


# endregion
