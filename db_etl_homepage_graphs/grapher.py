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
    from .utils import plot_thumbnail, plot_vaccinations, plot_vaccinations_50_plus
    from . import queries
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from db_etl_homepage_graphs.utils import (
        plot_thumbnail, plot_vaccinations, plot_vaccinations_50_plus
    )
    from db_etl_homepage_graphs import queries
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


METRICS = [
    'newAdmissions',
    'newCasesByPublishDate',
    'newDeaths28DaysByPublishDate',
    'newCasesBySpecimenDate',
    'newDeaths28DaysByDeathDate',
    'newVirusTestsByPublishDate'
]


def store_data(date: str, metric: str, svg: str, area_type: str = None,
               area_code: str = None, fiftyplus: bool = False):
    kws = dict(
        container="downloads",
        content_type="image/svg+xml",
        cache_control="public, max-age=30, s-maxage=90, must-revalidate",
        compressed=False
    )
    over50 = '_50plus' if fiftyplus else ''

    if not fiftyplus:
        path = f"homepage/{date}/thumbnail_{metric}.svg"

    if area_code is not None:
        path = f"homepage/{date}/{metric}/{area_type}/{area_code}{over50}_thumbnail.svg"

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
            metric=metric,
            datestamp=ts
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


def transform_50_plus(item):
    # TODO: create the function
    # These keys are expected: 'vaccination_date' and 'vaccination_date_percentage_dose'
    # in the output (calculated from 'payload')
    return {
        "area_type": 'string',
        "area_code": 'string',
        "date": 'string',
        "vaccination_date":  'payload',
        "vaccination_date_percentage_dose": 'payload'
    }


def get_vaccinations(date):
    ts = datetime.strptime(date, "%Y-%m-%d")
    partition = f"{ts:%Y_%-m_%-d}"

    vax_query = queries.VACCINATIONS_QUERY.format(partition_date=partition)
    vax_query_50_plus = queries.VACCINATIONS_QUERY_50_PLUS.format(partition=partition)

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(vax_query),
            datestamp=ts
        )
        values = resp.fetchall()

        resp50 = conn.execute(text(vax_query_50_plus))
        values_50_plus = resp50.fetchall()
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

    # Create 'waffle' for the 50+
    for item in values_50_plus:
        # item has got values as json
        # they have to be extracted/converted
        store_data(
            date,
            "vaccinations",
            plot_vaccinations(transform_50_plus(item)),
            area_type=item["area_type"],
            area_code=item["area_code"]
        )

    return True


def main(payload):
    category = payload.get("category", "main")

    if category == "main":
        for metric in METRICS:
            get_timeseries(payload['date'], metric)

    if payload.get("category") == "vaccination":
        get_vaccinations(payload['date'])

    return f'DONE: {payload["date"]}'


if __name__ == "__main__":
    main({"date": "2022-08-10"})
