#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from pathlib import Path
from datetime import datetime
from string import Template

# 3rd party:
from pandas import DataFrame
from sqlalchemy import text

# Internal:
try:
    from __app__.db_tables.covid19 import Session
    from __app__.caching import RedisClient
except ImportError:
    from db_tables.covid19 import Session
    from caching import RedisClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


REDIS_LANDING_PAGE_DB = 2

query_path = (
    Path(__file__)
    .parent
    .joinpath("query.sql")
    .resolve()
    .absolute()
)


def retrieve_data(timestamp: datetime):
    with open(query_path) as fp:
        query = fp.read()

    query = Template(query).substitute(release_date=f"{timestamp:%Y_%-m_%-d}")

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

    df = DataFrame(raw_data, columns=["key", "value"])

    return df


def main(payload):
    logging.info(f"Cache pre-population process triggered for: '{payload}'.")

    timestamp = datetime.fromisoformat(payload['timestamp'])

    data = retrieve_data(timestamp)

    with RedisClient(db=REDIS_LANDING_PAGE_DB) as client:
        data.apply(client.set_data, axis=1)

    logging.info(f"Done pre-populating the cache.")

    return f"DONE: {payload}"


if __name__ == '__main__':
    # Local run
    # ---------
    # WARNING:
    # Do not add today's date before release. Bad things will happen.
    #
    from datetime import timedelta

    # main({"timestamp": (datetime.now() - timedelta(days=1)).isoformat()})

    # Remove non-area data
    for db_ind in range(4, -1, -1):
        with RedisClient(db=db_ind) as redis_client:
            redis_client.delete_pattern(['*log*', '*announcement*'])
