#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:
from orjson import dumps
from sqlalchemy import text

# Internal:
try:
    from __app__.storage import StorageClient
    from .variables import PARAMETERS
    from __app__.db_tables.covid19 import Session
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from despatch_ops_workers.archive_dates.variables import PARAMETERS
    from database.postgres import Connection

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "generate_archive_dates"
]


def store_data(geo_data, container, path):
    payload = dumps(geo_data).decode().replace("NaN", "null")

    with StorageClient(
            container=container,
            path=path,
            content_type="application/json; charset=utf-8",
            cache_control="public, stale-while-revalidate=60, max-age=90",
            compressed=False,
            content_language=None
    ) as cli:
        cli.upload(payload)


def create_asset(data_type):
    params = PARAMETERS[data_type]

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(params["query"]),
            process_name=params["process_name"]
        )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    store_data(dict(raw_data), params['container'], params['path'])


def generate_archive_dates(payload):
    data_type = payload['data_type']

    create_asset(data_type)

    return f"DONE: Archive dates '{data_type}' - {payload['timestamp']}"


if __name__ == "__main__":
    generate_archive_dates({"data_type": "MAIN"})
