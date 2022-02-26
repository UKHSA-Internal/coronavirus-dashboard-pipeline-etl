#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import List
from os import getenv
from json import loads
from itertools import chain
from functools import partial
from datetime import datetime

# 3rd party:
from redis import Redis
from redis.client import Pipeline

from pandas import Series

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "RedisClient"
]


CREDENTIALS = loads(getenv("REDIS", "[]"))
API_ENV = getenv("API_ENV")


class RedisClient:
    """
    Client for handling operations on multiple Redis instances.
    """

    cache_ttl = 36 * 60 * 60  # 36 hours in seconds

    # This is to prevent data from expiring
    # during the weekend.
    if datetime.utcnow().isoweekday() >= 5:
        cache_ttl = 84 * 60 * 60  # 84 hours in seconds

    def __init__(self, db=0):
        logging.info(f"Initialised logging client - default TTL: {self.cache_ttl} seconds")

        self._instances: List[Redis] = list()
        self._pipelines: List[Pipeline] = list()
        self._db = db

        for index, creds in enumerate(CREDENTIALS):
            host, port, password = creds.split(";")
            cli = Redis(host=host, port=port, password=password, db=self._db)
            pipeline = cli.pipeline()

            self._pipelines.append(pipeline)
            self._instances.append(cli)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for pipeline, cli in zip(self._pipelines, self._instances):
            pipeline.execute()
            pipeline.close()
            cli.close()

    def _keys_from_pattern(self, conn, *patterns):
        fn = partial(conn.scan_iter, count=1000)
        return chain.from_iterable(map(fn, patterns))

    def set_data(self, data: Series, ttl=None):
        if ttl is None:
            ttl = self.cache_ttl

        for pipe in self._pipelines:
            pipe.set(data['key'], data['value'], ex=ttl)

    def delete_pattern(self, key_patterns: List[str]):
        for pipe, conn in zip(self._pipelines, self._instances):
            for key in self._keys_from_pattern(conn, *key_patterns):
                pipe.delete(key)

    def delete_keys(self, keys: List[str]):
        for pipe in self._pipelines:
            for key in keys:
                pipe.delete(key)

    def expire_keys(self, ttl: List[int], keys: List[str]):
        for pipe, expiry in zip(self._pipelines, ttl):
            for key in keys:
                pipe.expire(key, expiry)

    def expire_pattern(self, ttl: List[int], key_patterns: List[str]):
        for pipe, conn, expiry in zip(self._pipelines, self._instances, ttl):
            for key in self._keys_from_pattern(conn, *key_patterns):
                pipe.expire(key, expiry)

    def flush_db(self):
        for pipe in self._pipelines:
            pipe.flushdb()

    def delete_hash_fields(self, key: str, *fields):
        for pipe in self._pipelines:
            pipe.hdel(key, *fields)
