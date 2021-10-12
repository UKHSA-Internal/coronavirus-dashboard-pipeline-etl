#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import List
from os import getenv
from json import loads

# 3rd party:
from redis import Redis
from redis.client import Pipeline

from pandas import Series

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "RedisClient"
]

CACHE_TTL = 36 * 60 * 60  # 36 hours in seconds

CREDENTIALS = loads(getenv("REDIS", "[]"))


class RedisClient:
    """
    Client for handling operations on multiple Redis instances.
    """

    def __init__(self):
        self._instances: List[Redis] = list()
        self._pipelines: List[Pipeline] = list()

    def __enter__(self):
        for creds in CREDENTIALS:
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

    def set_data(self, data: Series, ttl=CACHE_TTL):
        for pipe in self._pipelines:
            pipe.set(data['key'], data['value'], ex=ttl)

    def delete(self, key_pattern):
        for pipe in self._pipelines:
            for conn in self._instances:
                for key in conn.keys(key_pattern):
                    pipe.delete(key)
