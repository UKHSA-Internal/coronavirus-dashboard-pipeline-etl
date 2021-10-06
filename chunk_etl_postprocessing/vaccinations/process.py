#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from datetime import datetime, timedelta

# 3rd party:
from pandas import DataFrame
from sqlalchemy import text

# Internal:
try:
    from __app__.db_tables.covid19 import Session
    from __app__.db_etl_upload import generate_row_hash, to_sql
    from .queries import PUBLISH_DATE_CALCULATION, PERCENTAGE_DATA
except ImportError:
    from db_tables.covid19 import Session
    from db_etl_upload import generate_row_hash, to_sql
    from chunk_etl_postprocessing.vaccinations.queries import (
        PUBLISH_DATE_CALCULATION, PERCENTAGE_DATA
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'process_vaccinations'
]


def derive_publish_date_metrics(timestamp, area_type):
    query = PUBLISH_DATE_CALCULATION.format(
        today=f"{timestamp:%Y_%-m_%-d}",
        yesterday=f"{timestamp - timedelta(days=1):%Y_%-m_%-d}",
        area_type=area_type
    )

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(text(query), )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    columns = [
        'partition_id', 'area_id', 'area_type', 'area_code',
        'metric_id', 'release_id', 'date', 'payload'
    ]

    to_sql(
        DataFrame(raw_data, columns=columns)
        .pipe(lambda dt: dt.assign(hash=generate_row_hash(dt, hash_only=True)))
        .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
    )

    return True


def derive_publish_date_percentages(timestamp, area_type):
    query = PERCENTAGE_DATA.format(
        date=f"{timestamp:%Y_%-m_%-d}",
        area_type=area_type
    )

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
        'area_id', 'partition_id', 'metric_id', 'metric', 'area_type',
        'area_code', 'release_id', 'date', 'payload'
    ]

    to_sql(
        DataFrame(raw_data, columns=columns)
        .pipe(lambda dt: dt.assign(hash=generate_row_hash(dt, hash_only=True)))
        .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
    )

    return True


def process_vaccinations(payload):
    timestamp = datetime.fromisoformat(payload['timestamp'])
    area_types = ["utla", "ltla"]

    for area_type in area_types:
        derive_publish_date_metrics(timestamp, area_type)
        derive_publish_date_percentages(timestamp, area_type)

    return f"DONE: {payload['timestamp']}"


if __name__ == '__main__':
    process_vaccinations({
        "timestamp": (datetime.utcnow() - timedelta(days=1)).isoformat()
    })

