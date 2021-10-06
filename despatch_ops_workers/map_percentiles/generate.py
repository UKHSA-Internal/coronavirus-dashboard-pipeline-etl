#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime

# 3rd party:
from pandas import DataFrame
from orjson import dumps
from sqlalchemy import text

# Internal:
try:
    from __app__.storage import StorageClient
    from .variables import PARAMETERS
    from ..utils.variables import AREA_TYPE_PARTITION
    from __app__.db_tables.covid19 import Session
except ImportError:
    from storage import StorageClient
    from despatch_ops_workers.map_percentiles.variables import PARAMETERS
    from despatch_ops_workers.utils.variables import AREA_TYPE_PARTITION
    from db_tables.covid19 import Session

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "generate_percentiles"
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


def create_asset(area_type: str, release_date: str):
    params = PARAMETERS[area_type]
    partition_date = datetime.fromisoformat(release_date).strftime("%Y_%-m_%-d")

    query = params['query'].format(
        date=partition_date,
        area_type=AREA_TYPE_PARTITION[area_type],
        attr=params['attribute']
    )

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(query),
            metric=params["metric"],
            area_type=area_type
        )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    percentiles = (
        DataFrame(raw_data, columns=["date", "min", "first", "second", "third", "max"])
        .set_index("date").T
        .to_dict()
    )

    store_data(percentiles, params['container'], params['path'])


def generate_percentiles(payload):
    area_type = payload["area_type"]
    timestamp = payload["timestamp"]

    create_asset(area_type, timestamp)

    return f"DONE: percentiles {area_type}::{timestamp}"


if __name__ == "__main__":
    generate_percentiles({
        "area_type": "nation",
        "release_timestamp": datetime.utcnow().isoformat()
    })
