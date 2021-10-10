#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging

# 3rd party:
from azure.functions import HttpRequest, HttpResponse
from sqlalchemy import text

# Internal: 
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]


def main(req: HttpRequest) -> HttpResponse:
    logging.info("Running health checks")

    logging.info("Testing database connection and response")
    session = Session()
    conn = session.connection()
    try:
        q = conn.execute(text("SELECT 1 AS result;"))
        _ = q.fetchone()
        logging.info("Database is alive")
    except Exception as err:
        session.rollback()
        logging.warning("Database is dead")
        raise err
    finally:
        session.close()

    storage_kws = dict(
        container="pipeline",
        path="healthchecks/etl",
        content_type="text/plain",
        compressed=False
    )
    logging.info("Testing storage connection")
    try:
        with StorageClient(**storage_kws) as storage:
            storage.upload("test")
            storage.download()
            storage.delete()
        logging.info("Storage is alive")
    except Exception as err:
        logging.warning("Storage is dead")
        raise err

    logging.info("All tests passed.")

    return HttpResponse(b"ALIVE", status_code=200)
