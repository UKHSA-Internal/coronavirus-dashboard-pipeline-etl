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
    from __app__.db_tables.covid19 import Session
    from __app__.despatch_ops_workers.map_vaccinations_geojson.queries import QUERY
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from despatch_ops_workers.map_vaccinations_geojson.queries import QUERY

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "generate_geojson"
]


def store_data(geo_data, container, path):
    payload = dumps(geo_data).decode().replace("NaN", "null")

    with StorageClient(
            container=container,
            path=path,
            content_type="application/json; charset=utf-8",
            cache_control="public, stale-while-revalidate=60, max-age=90",
            compressed=True,
            content_language=None
    ) as cli:
        cli.upload(payload)


def create_asset(release_date: str):

    partition_date = datetime.fromisoformat(release_date).strftime("%Y_%-m_%-d")

    query = QUERY.format(date=partition_date)

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

    data = DataFrame(raw_data, columns=["properties", "geometry"])

    geo_data = {
        "type": "FeatureCollection",
        "features": (
            data
            .assign(type="Feature")
            .reset_index()
            .rename(columns={"index": "id"})
            .to_dict("records")
        )
    }

    return store_data(geo_data, "downloads", "maps/vax-data_latest.geojson")


def generate_geojson(payload):
    timestamp = payload["timestamp"]

    create_asset(timestamp)

    return f"DONE {timestamp}"


if __name__ == "__main__":
    from datetime import timedelta

    generate_geojson({
        "timestamp": (datetime.utcnow() - timedelta(days=0)).isoformat()
    })
