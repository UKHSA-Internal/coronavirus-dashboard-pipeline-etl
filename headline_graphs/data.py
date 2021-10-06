#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from string import Template
from datetime import datetime

# 3rd party:

# Internal:
try:
    from __app__.database import CosmosDB, Collection
except ImportError:
    from database import CosmosDB, Collection

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_data'
]


START_DATE = "2020-04-01"

QUERY = Template("""\
SELECT
    VALUE {
        'date':  c.date, 
        'value': c.$metric,
        'rollingSum': c.${metric}RollingSum
    }
FROM     c
WHERE    c.releaseTimestamp = @releaseTimestamp
     AND c.areaNameLower    = @areaName
     AND c.date >= @startDate
     AND IS_DEFINED(c.$metric)
ORDER BY c.releaseTimestamp DESC,
         c.date             DESC,
         c.areaType         ASC,
         c.areaNameLower    ASC\
""")


data_db = CosmosDB(Collection.DATA)


def process_dates(date: str):
    result: dict = {
        'date': datetime.strptime(date, "%Y-%m-%d"),
    }

    result['formatted'] = result['date'].strftime('%-d %B %Y')

    return result


def get_data(timestamp: str, area_name: str, metric: str):
    """
    Retrieves the last fortnight worth of ``metric`` values
    for ``areaName`` as released on ``timestamp``.
    """
    query = QUERY.substitute({
        "metric": metric,
        "areaName": area_name
    })

    params = [
        {"name": "@releaseTimestamp", "value": timestamp},
        {"name": "@areaName", "value": area_name.lower()},
        {"name": "@startDate", "value": START_DATE}
    ]

    result = [
        {**row, **process_dates(row["date"])}
        for row in data_db.query_iter(query, params=params)
    ]

    return result
