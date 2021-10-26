#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import Awaitable
from datetime import datetime
from asyncio import gather, get_event_loop

# 3rd party:
from sqlalchemy import text

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
    from .utils import plot_thumbnail, plot_vaccinations
    from . import queries
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from db_etl_homepage_graphs.utils import plot_thumbnail, plot_vaccinations
    from db_etl_homepage_graphs import queries
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


metrics = [
    'newAdmissions',
    'newCasesByPublishDate',
    'newDeaths28DaysByPublishDate',
    'newVirusTests'
]


def store_data(date: str, metric: str, svg: str, area_type: str = None,
               area_code: str = None):
    kws = dict(
        container="downloads",
        content_type="image/svg+xml",
        cache_control="public, max-age=30, s-maxage=90, must-revalidate",
        compressed=False
    )

    path = f"homepage/{date}/thumbnail_{metric}.svg"

    if area_code is not None:
        path = f"homepage/{date}/{metric}/{area_type}/{area_code}_thumbnail.svg"

    with StorageClient(path=path, **kws) as cli:
        cli.upload(svg)


def get_timeseries(date: str, metric: str):
    ts = datetime.strptime(date, "%Y-%m-%d")
    partition = f"{ts:%Y_%-m_%-d}_other"
    partition_id = f"{ts:%Y_%-m_%-d}|other"
    values_query = queries.TIMESRIES_QUERY.format(partition=partition)
    change_query = queries.LATEST_CHANGE_QUERY.format(partition=partition)

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(values_query),
            partition_id=partition_id,
            metric=metric
        )
        values = resp.fetchall()

        resp = conn.execute(
            text(change_query),
            partition_id=partition_id,
            datestamp=ts,
            metric=metric + "Change"
        )
        change = resp.fetchone()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    store_data(
        date,
        metric,
        plot_thumbnail(values, metric_name=metric, change=change)
    )

    return True


def get_vaccinations(date):
    ts = datetime.strptime(date, "%Y-%m-%d")
    partition = f"{ts:%Y_%-m_%-d}"

    vax_query = queries.VACCINATIONS_QUERY.format(partition_date=partition)

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(vax_query),
            datestamp=ts
        )
        values = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    for item in values:
        store_data(
            date,
            "vaccinations",
            plot_vaccinations(item),
            area_type=item["area_type"],
            area_code=item["area_code"]
        )

    return True


def main(payload):
    category = payload.get("category", "main")

    if category == "main":
        for metric in metrics:
            get_timeseries(payload['date'], metric)

    if payload.get("category") == "vaccination":
        get_vaccinations(payload['date'])

    return f'DONE: {payload["date"]}'


if __name__ == "__main__":
    main({"date": "2021-05-17"})
