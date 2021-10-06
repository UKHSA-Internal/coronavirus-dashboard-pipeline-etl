#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from pathlib import Path
from datetime import datetime
from os import getenv
from string import Template
from json import loads
from redis import Redis
from redis.client import Pipeline
from typing import List

# 3rd party:
from pandas import DataFrame, Series
from sqlalchemy import text

# Internal:
try:
    from __app__.db_tables.covid19 import Session
    from __app__.db_etl_upload import generate_row_hash, to_sql
except ImportError:
    from db_tables.covid19 import Session
    from db_etl_upload import generate_row_hash, to_sql

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


query_path = (
    Path(__file__)
    .parent
    .joinpath("query.sql")
    .resolve()
    .absolute()
)

CACHE_TTL = 36 * 60 * 60  # 36 hours in seconds


class RedisClient:
    def __init__(self):
        self._redis_creds = loads(getenv("REDIS", "[]"))
        self._instances: List[Redis] = list()
        self._pipelines: List[Pipeline] = list()

    def __enter__(self):
        for creds in self._redis_creds:
            host, port, password = creds.split(";")
            cli = Redis(host=host, port=port, password=password, db=2)
            pipeline = cli.pipeline()
            self._pipelines.append(pipeline)
            self._instances.append(cli)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for pipeline, cli in zip(self._pipelines, self._instances):
            pipeline.execute()
            pipeline.close()
            cli.close()

    def set_data(self, data: Series):
        for pipe in self._pipelines:
            pipe.set(data['key'], data['value'], ex=CACHE_TTL)

    def delete(self):
        for pipe in self._pipelines:
            for conn in self._instances:
                for key in conn.keys("[^area]*"):
                    pipe.delete(key)


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

    with RedisClient() as redis_client:
        data.apply(redis_client.set_data, axis=1)

    logging.info(f"Done pre-populating the cache.")

    return f"DONE: {payload}"


if __name__ == '__main__':
    # Local run
    # ---------
    # WARNING:
    # Do not add today's date before release. Bad things will happen.
    from datetime import timedelta

    main({"timestamp": (datetime.now() - timedelta(days=0)).isoformat()})

    # with RedisClient() as redis_client:
    #     redis_client.delete()
