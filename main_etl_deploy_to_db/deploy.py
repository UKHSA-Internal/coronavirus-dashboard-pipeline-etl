#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from json import loads

# 3rd party:

# Internal:
try:
    from __app__.fanout import enqueue_job
    from __app__.utilities.generic_types import ETLResponse
except ImportError:
    from fanout import enqueue_job
    from utilities.generic_types import ETLResponse

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def main(payload):
    payload = ETLResponse(**payload)

    if payload.environment != "STAGING":
        return f"Not processed - Environment: {payload.environment}"

    queue_kws = dict(
        module="db_etl",
        handler="combine_and_upload_from_file",
        container="pipeline",
        date=payload.date
    )

    enqueue_job(
        **queue_kws,
        area_type=payload.area_type,
        area_code=payload.area_code
    )

    return "SUCCESS"
