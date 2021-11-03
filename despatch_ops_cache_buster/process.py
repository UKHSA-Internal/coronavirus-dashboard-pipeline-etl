#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import Iterator
from enum import IntEnum

# 3rd party:

# Internal:
try:
    from __app__.caching import RedisClient
except ImportError:
    from caching import RedisClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_operations',
    'main'
]


class CacheDatabase(IntEnum):
    APIM = 0
    GREENHOUSE = 1
    SUMMARY = 2
    GENERIC_API = 3


# First level key is the DB number.
# Second level key is a `RedisClient` method name.
# Third level key is the payload for the method.
CACHE_OPERATIONS = {
    CacheDatabase.GENERIC_API: {
        "delete_pattern": {
            "key_patterns": [
                "*v[12]/data*",
                "*/generic/soa/msoa/*"
            ]
        },
    },
    CacheDatabase.SUMMARY: {
        "delete_pattern": {
            "key_patterns": [
                "[^area]*"
            ]
        }
    }
}


def get_operations() -> Iterator:
    for db, operations in CACHE_OPERATIONS.items():
        yield {"db": db, "operations": operations}


def main(payload):
    with RedisClient(db=payload['db']) as cli:
        for method, args in payload['operations'].items():
            func = getattr(cli, method)

            if isinstance(args, dict):
                func(**args)
            elif isinstance(args, (list, str, int, float)):
                func(args)
            else:
                raise TypeError(
                    "expected one of dict, list, str, int, or float, "
                    "got '%s' instead" % type(args)
                )

    return f"DONE: {payload}"
