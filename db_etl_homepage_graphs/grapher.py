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
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from db_etl_homepage_graphs.utils import plot_thumbnail, plot_vaccinations

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


TIMESRIES_QUERY = """\
SELECT
     date                           AS "date",
     (payload ->> 'value')::NUMERIC AS "value"
FROM covid19.time_series_p{partition} AS main
JOIN covid19.release_reference AS rr ON rr.id = release_id
JOIN covid19.metric_reference  AS mr ON mr.id = metric_id
JOIN covid19.area_reference    AS ar ON ar.id = main.area_id
WHERE
      partition_id = :partition_id
  AND area_type = 'overview'
  AND date > ( NOW() - INTERVAL '6 months')
  AND metric = :metric
ORDER BY date DESC;\
"""

LATEST_CHANGE_QUERY = """\
SELECT
     metric,
     date                        AS "date",
     (payload -> 'value')::FLOAT AS "value"
FROM covid19.time_series_p{partition} AS ts
JOIN covid19.release_reference AS rr ON rr.id = release_id
JOIN covid19.metric_reference  AS mr ON mr.id = metric_id
JOIN covid19.area_reference    AS ar ON ar.id = ts.area_id
WHERE
      partition_id = :partition_id
  AND area_type = 'overview'
  AND date > ( DATE( :datestamp ) - INTERVAL '30 days' )
  AND metric = :metric
ORDER BY date DESC
OFFSET 0
FETCH FIRST 1 ROW ONLY;\
"""


VACCINATIONS_QUERY = """\
SELECT first.area_type,
       first.area_code,
       MAX(first.date)              AS date,
       MAX(FLOOR(first_dose))::INT  AS first_dose,
       MAX(FLOOR(second_dose))::INT AS second_dose
FROM (
         SELECT area_type,
                area_code,
                MAX(date)                        AS date,
                MAX((payload -> 'value')::FLOAT) AS first_dose
         FROM (
                  SELECT *
                  FROM covid19.time_series_p{partition_date}_other AS tm
                           JOIN covid19.release_reference AS rr ON rr.id = release_id
                           JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                           JOIN covid19.area_reference AS ar ON ar.id = tm.area_id
                  UNION
                  (
                      SELECT *
                      FROM covid19.time_series_p{partition_date}_utla AS ts
                               JOIN covid19.release_reference AS rr ON rr.id = release_id
                               JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                               JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                  )
                  UNION
                  (
                      SELECT *
                      FROM covid19.time_series_p{partition_date}_ltla AS ts
                               JOIN covid19.release_reference AS rr ON rr.id = release_id
                               JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                               JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
                  )
              ) AS ts
         WHERE date > (DATE(:datestamp) - INTERVAL '30 days')
           AND metric = 'cumVaccinationFirstDoseUptakeByPublishDatePercentage'
           AND (payload ->> 'value') NOTNULL
         GROUP BY area_type, area_code
     ) as first
         JOIN (
    SELECT area_type,
           area_code,
           MAX(date)                        AS date,
           MAX((payload -> 'value')::FLOAT) AS second_dose
    FROM (
             SELECT *
             FROM covid19.time_series_p{partition_date}_other AS tm
                      JOIN covid19.release_reference AS rr ON rr.id = release_id
                      JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                      JOIN covid19.area_reference AS ar ON ar.id = tm.area_id
             UNION
             (
                 SELECT *
                 FROM covid19.time_series_p{partition_date}_utla AS ts
                          JOIN covid19.release_reference AS rr ON rr.id = release_id
                          JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                          JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
             )
             UNION
             (
                 SELECT *
                 FROM covid19.time_series_p{partition_date}_ltla AS ts
                          JOIN covid19.release_reference AS rr ON rr.id = release_id
                          JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                          JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
             )
         ) AS ts
    WHERE date > ( DATE(:datestamp) - INTERVAL '30 days' )
      AND metric = 'cumVaccinationSecondDoseUptakeByPublishDatePercentage'
      AND (payload ->> 'value') NOTNULL
    GROUP BY area_type, area_code
) AS second ON first.date = second.date AND first.area_code = second.area_code
GROUP BY first.area_type, first.area_code;\
"""


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
    values_query = TIMESRIES_QUERY.format(partition=partition)
    change_query = LATEST_CHANGE_QUERY.format(partition=partition)

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

    vax_query = VACCINATIONS_QUERY.format(partition_date=partition)

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
