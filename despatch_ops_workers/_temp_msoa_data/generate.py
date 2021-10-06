#!/usr/bin python3


# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from datetime import datetime

# 3rd party:
from pandas import DataFrame
from sqlalchemy import text

# Internal: 
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
    from __app__.database import CosmosDB, Collection
    from .queries import MAIN
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from database import CosmosDB, Collection
    from despatch_ops_workers.temp_msoa_data.queries import MAIN

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_msoa_data'
]


db_container = CosmosDB(Collection.WEEKLY, writer=True)


def upsert(content):
    try:
        db_container.upsert(content)
        return None
    except Exception as e:
        logging.error(e)


def get_data(timestamp):
    query = MAIN.format(date=f"{timestamp:%Y_%-m_%-d}")

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(text(query))
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    columns = [
        "id", "areaCode", "areaType", "areaName",
        "latest", "newCasesBySpecimenDate", "release"
    ]

    return DataFrame(raw_data, columns=columns)


def generate_msoa_data(payload):
    timestamp = datetime.fromisoformat(payload["timestamp"])
    data = get_data(timestamp)

    for item in data.to_dict(orient="records"):
        upsert(item)

    return f"DONE: msoa data deployment {payload['timestamp']}"


if __name__ == "__main__":
    generate_msoa_data({"timestamp": datetime.utcnow().isoformat()})
