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
    from .queries import PUBLISH_DATE_CALCULATION
except ImportError:
    from db_tables.covid19 import Session
    from db_etl_upload import generate_row_hash, to_sql
    from chunk_etl_postprocessing.testing.queries import PUBLISH_DATE_CALCULATION

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'process_testing'
]


def derive_publish_date_metrics(timestamp):
    today = f"{timestamp:%Y_%-m_%-d}"
    yesterday = f"{timestamp - timedelta(days=1):%Y_%-m_%-d}"

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(PUBLISH_DATE_CALCULATION),
            today_partitions=[f"{today}|other", f"{today}|utla", f"{today}|ltla"],
            yesterday_partitions=[f"{yesterday}|other", f"{yesterday}|utla", f"{yesterday}|ltla"],
        )
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


def process_testing(payload):
    timestamp = datetime.fromisoformat(payload['timestamp'])
    derive_publish_date_metrics(timestamp)

    return f"DONE: {payload['timestamp']}"


if __name__ == '__main__':
    process_testing({
        "timestamp": (datetime.utcnow() - timedelta(days=0)).isoformat()
    })

