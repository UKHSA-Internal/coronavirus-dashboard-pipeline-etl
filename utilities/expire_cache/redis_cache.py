#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv
import ssl

# 3rd party:
from redis import StrictRedis
import certifi
from numpy.random import randint
import ssl

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'flush_batch',
    'expire_uniformly'
]


ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.check_hostname = True
ssl_context.load_default_certs()


async def flush_batch(prefix, batch_size=5000):
    redis_client = StrictRedis(
        host=getenv("RedisURL"),
        port=int(getenv("RedisPort")),
        db=0,
        password=getenv("RedisKey"),
        ssl=True,
        ssl_ca_certs=certifi.where()
    )

    cursor = '0'
    ns_keys = f"{prefix}*"

    while cursor != 0:
        cursor, keys = redis_client.scan(cursor=cursor, match=ns_keys, count=batch_size)

        if keys:
            redis_client.delete(*keys)

    return True


def expire_uniformly(expire_in, match="*"):
    redis_client = StrictRedis(
        host=getenv("RedisURL"),
        port=int(getenv("RedisPort")),
        db=0,
        password=getenv("RedisKey"),
        ssl=True,
        ssl_ca_certs=certifi.where()
    )

    for key in redis_client.scan_iter(match):
        redis_client.expire(key, randint(1, expire_in))

    redis_client.close()

    return True
