#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       26 Dec 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import List
from random import random
from time import sleep
from os import getenv
from datetime import datetime
from typing import NamedTuple, Union

# 3rd party:
from orjson import loads, dumps

# Internal:
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]

DEBUG = getenv("DEBUG", False)

RENAME = {
    # from : to
    # "plannedPillarOneTwoFourCapacityByPublishDate": "capacityPillarOneTwoFour",
    "plannedPillarOneTwoCapacityByPublishDate": "capacityPillarOneTwo",
    "plannedPillarThreeCapacityByPublishDate": "capacityPillarThree",
    "plannedPillarOneCapacityByPublishDate": "capacityPillarOne",
    "plannedPillarTwoCapacityByPublishDate": "capacityPillarTwo",
    "plannedPillarFourCapacityByPublishDate": "capacityPillarFour",
    'newLFDTestsBySpecimenDate': 'newLFDTests',
    'cumLFDTestsBySpecimenDate': 'cumLFDTests',
    'newVirusTestsByPublishDate': 'newVirusTests',
    'cumVirusTestsByPublishDate': 'cumVirusTests',
    # '"cumVaccinationFirstDoseUptakeByPublishDate"': '"cumVaccinationFirstDoseUptakeByPublishDatePercentage"',
    # '"cumVaccinationSecondDoseUptakeByPublishDate"': '"cumVaccinationSecondDoseUptakeByPublishDatePercentage"',
    # '"cumVaccinationCompleteCoverageByPublishDate"': '"cumVaccinationCompleteCoverageByPublishDatePercentage"',
    'nations': 'nation',
    'nhsTrusts': 'nhsTrust',
    'utlas': 'utla',
    'ltlas': 'ltla',
    'nhsRegions': 'nhsRegion',
    'regions': 'region',
    'uk': 'overview'
}

RAW_DATA_CONTAINER = "rawdbdata"

SEEN_FILE_KWS = dict(
    container="pipeline",
    path="info/seen",
    content_type="text/plain; charset=utf-8",
    compressed=False,
    cache_control="no-cache, max-age=0",
    tier='Cool'
)

CHUNK_KWS = dict(
    container="pipeline",
    content_type="application/json; charset=utf-8",
    cache_control="no-cache, max-age=0",
    compressed=False,
    tier='Cool'
)


class RetrieveDataType(NamedTuple):
    data_path: str
    timestamp: Union[str, None] = None
    legacy: bool = False


def main(payload) -> List[str]:
    payload = RetrieveDataType(**payload)
    logging.info(f"\tFile name loaded from the request body: {payload.data_path}")

    # ---------------------------------------------------
    # Sleep for a random period to allow de-sync
    # concurrent triggers.
    # sleep_time = random() * 10
    # sleep(sleep_time)

    # Duplication safeguard.
    # if not DEBUG and payload.legacy is False:
    #     with StorageClient(**SEEN_FILE_KWS) as client:
    #         seen_data = client.download().readall().decode()
    #
    #         if seen_data == payload.data_path:
    #             raise RuntimeError("File has already been processed")
    #
    #         client.upload(payload.data_path)

    # ---------------------------------------------------
    # Downloading raw data.
    with StorageClient(container=RAW_DATA_CONTAINER, path=payload.data_path) as client:
        raw_data = client.download().readall().decode()

    logging.info(f"\tDownloaded JSON data")

    # ---------------------------------------------------
    # Renaming the data based on the "RENAME" dictionary.
    logging.info(f'\tRunning the "rename" process')

    for key, category_content in RENAME.items():
        logging.info(f'\t\t> from "{key}" to "{category_content}"')
        raw_data = raw_data.replace(f'"{key}"', f'"{category_content}"')

    logging.info(f'\t> "rename" process complete')

    # ---------------------------------------------------
    # Loading the data
    logging.info(f'\tStarting to parse JSON data')
    data_dict = loads(raw_data)

    logging.info(
        f'\tJSON data successfully parsed. Submitting '
        f'chunks for processing: {list(data_dict.keys())}'
    )
    # ---------------------------------------------------

    date = datetime.fromisoformat(payload.timestamp[:26])

    data_paths = list()

    for category, category_content in data_dict.items():
        for area_code, area_content in category_content.items():
            path = f"etl/transit/{date:%Y-%m-%d_%H%M}/{category}_{area_code}.json"

            payload = {
                category: {
                    area_code: area_content
                }
            }

            with StorageClient(**CHUNK_KWS, path=path) as client:
                json_data = dumps(payload)
                client.upload(json_data.decode())

            data_paths.append(path)

    return data_paths
