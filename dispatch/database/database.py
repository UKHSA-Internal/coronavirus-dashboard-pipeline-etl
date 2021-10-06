#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       22 June 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import NamedTuple
from os import getenv
from json import dumps

# 3rd party:
from azure.cosmos import cosmos_client

# Internal:


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_latest_timestamp',
    'get_latest_count',
    'get_legacy_cases',
    'get_legacy_deaths',
    'get_count_for_metric',
    'get_latest_release_date'
]


class Credentials(NamedTuple):
    host = getenv('AzureCosmosHost')
    key = getenv('AzureCosmosKey')
    db_name = getenv('AzureCosmosDBName')
    collection = getenv('AzureCosmosCollection')


client = cosmos_client.CosmosClient(
    url=Credentials.host,
    credential={'masterKey': Credentials.key},
    user_agent="CosmosDBDotnetQuickstart",
    user_agent_overwrite=True
)

db = client.get_database_client(Credentials.db_name)
container = db.get_container_client(Credentials.collection)


def get_latest_timestamp() -> str:
    """
    Retrieves the data from the database.

    Returns
    -------
    str
    """
    query = """\
SELECT      VALUE MAX(c.releaseTimestamp) 
FROM        c 
"""

    items = container.query_items(
        query=query,
        enable_cross_partition_query=True,
        max_item_count=1
    )

    results = list(items)

    return results[0]


def get_latest_release_date() -> str:
    """
    Retrieves the data from the database.

    Returns
    -------
    str
    """
    query = """\
SELECT      VALUE MAX(c.seriesDate) 
FROM        c 
"""

    items = container.query_items(
        query=query,
        enable_cross_partition_query=True,
        max_item_count=1
    )

    results = list(items)

    return results[0]


def get_latest_count() -> int:
    query = """\
SELECT      VALUE COUNT(1)
FROM        c 
WHERE       c.releaseTimestamp = @releaseTimestamp
"""

    items = container.query_items(
        query=query,
        parameters=[
            {"name": "@releaseTimestamp", "value": get_latest_timestamp()}
        ],
        enable_cross_partition_query=True,
        max_item_count=1
    )

    results = list(items)

    return int(results[0])


def get_legacy_cases():
    from pandas import read_json

    query = """\
SELECT 
    VALUE {
        'Area name':                           c.areaName,
        'Area code':                           c.areaCode,
        'Area type':                           c.areaType,
        'Specimen date':                       c.date,
        'Daily lab-confirmed cases':           (c.newCasesBySpecimenDate     ?? null),
        'Cumulative lab-confirmed cases':      (c.cumCasesBySpecimenDate     ?? null),
        'Cumulative lab-confirmed cases rate': (c.cumCasesBySpecimenDateRate ?? null)
    }
FROM   c
WHERE  c.releaseTimestamp = @releaseTimestamp
   AND (
          c.areaType = @areaTypeA
       OR c.areaType = @areaTypeB
       OR c.areaType = @areaTypeC
       OR c.areaType = @areaTypeD
   ) 
   AND IS_DEFINED(c.newCasesBySpecimenDate)
   AND STARTSWITH(c.areaCode, @areaCodeStartsWith) 
"""
    # cases: England, region, utla, ltla (Code beginning with E)
    # deaths: nation + uk (with areaType as UK)

    items = container.query_items(
        query=query,
        parameters=[
            {"name": "@releaseTimestamp", "value": get_latest_timestamp()},
            {"name": "@areaTypeA", "value": "nation"},
            {"name": "@areaTypeB", "value": "region"},
            {"name": "@areaTypeC", "value": "utla"},
            {"name": "@areaTypeD", "value": "ltla"},
            {"name": "@areaCodeStartsWith", "value": "E"},
        ],
        enable_cross_partition_query=True,
        max_item_count=10000
    )

    json_data = dumps(list(items))
    data = read_json(json_data, orient='records')
    data = data.replace({"overview": "UK"})

    return data.to_csv(float_format="%g", index=None)


def get_legacy_deaths():
    from pandas import read_json

    query = """\
SELECT 
    VALUE {
        'Area name':              c.areaName,
        'Area code':              c.areaCode,
        'Area type':              c.areaType,
        'Reporting date':         c.date,
        'Daily change in deaths': (c.newDeathsByPublishDate     ?? null),
        'Cumulative deaths':      (c.cumDeathsByPublishDate     ?? null)
    }
FROM   c
WHERE  c.releaseTimestamp = @releaseTimestamp
   AND (
          c.areaType = @areaTypeA
       OR c.areaType = @areaTypeB
   )
   AND IS_DEFINED(c.newDeathsByPublishDate)
"""

    items = container.query_items(
        query=query,
        parameters=[
            {"name": "@releaseTimestamp", "value": get_latest_timestamp()},
            {"name": "@areaTypeA", "value": "overview"},
            {"name": "@areaTypeB", "value": "nation"},
        ],
        enable_cross_partition_query=True,
        max_item_count=10000
    )

    json_data = dumps(list(items))
    data = read_json(json_data, orient='records')
    data = data.replace({"overview": "UK"})

    return data.to_csv(float_format="%g", index=None)


def get_count_for_metric(metric):
    query = f"""\
SELECT VALUE COUNT(c.{metric})
FROM c
WHERE 
        c.releaseTimestamp = @releaseTimestamp
    AND IS_DEFINED(c.{metric})
    AND ARRAY_LENGTH(c.{metric}) > 0\
    """

    items = container.query_items(
        query=query,
        parameters=[
            {"name": "@releaseTimestamp", "value": get_latest_timestamp()},
        ],
        enable_cross_partition_query=True,
        max_item_count=10000
    )

    return list(items).pop(0)
