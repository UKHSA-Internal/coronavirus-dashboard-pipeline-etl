#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       30 Sep 2021
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import TypedDict, Union
from datetime import datetime
import re

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "FilePathData",
    "parse_filepath",
    "category_label"
]


category2process_name = {
    ("vaccination", None): "VACCINATION",
    ("positivity", None): "POSITIVITY & PEOPLE TESTED",
    ("healthcare", None): "HEALTHCARE",
    ("tests", None): 'TESTING: MAIN',
    ("cases", None): 'CASES: MAIN',
    ("deaths", None): 'DEATHS: MAIN',
    ("main", None): "MAIN",
    ("msoa", None): "MSOA",
    ('cases-by-specimen-date', 'MSOA'): "MSOA",
    ("vaccinations-by-vaccination-date", "MSOA"): "MSOA: VACCINATION - EVENT DATE",
    ("cases-by-specimen-date", None): {
        "age-demographics": "AGE DEMOGRAPHICS: CASE - EVENT DATE"
    },
    ("deaths28days-by-death-date", None): {
        "age-demographics": "AGE-DEMOGRAPHICS: DEATH28DAYS - EVENT DATE"
    },
    ("vaccinations-by-vaccination-date", None): {
        "age-demographics": "AGE-DEMOGRAPHICS: VACCINATION - EVENT DATE"
    },
    ("first-episodes-by-specimen-date", None): {
        "age-demographics": "AGE-DEMOGRAPHICS: CASES - FIRST EPISODES"
    },
    ("reinfections-by-specimen-date", None): {
        "age-demographics": "AGE-DEMOGRAPHICS: CASES - REINFECTIONS"
    },
    ("variants", None): {
        "episodes": "EPISODE VARIANTS - EPISODES"
    },
}

filename_pattern = re.compile(
    r"""
    ^(?P<prefix>[0-9-]{10})/
    (?P<area_type>MSOA)?_?
    (?P<category>[a-z0-9-]*)_?
    (?P<subcategory>[a-z0-9-]*)_
    (?P<timestamp>\d{12})\.parquet$
    """,
    re.IGNORECASE | re.VERBOSE
)


class FilePathData(TypedDict):
    date: Union[str, None]
    area_type: Union[str, None]
    category: Union[str, None]
    subcategory: Union[str, None]
    timestamp: str


def parse_filepath(filepath: str) -> Union[FilePathData, None]:
    found = filename_pattern.search(filepath)

    if found is None:
        main_found = re.search(r"data_(\d+).json", "data_202109301358.json")

        if main_found is None:
            return None

        timestamp_raw = main_found.group(1)
        timestamp = datetime.strptime(timestamp_raw, "%Y%m%d%H%M")
        return FilePathData(
            date=f"{timestamp:%Y-%m-%d}",
            area_type=None,
            category="main",
            subcategory=None,
            timestamp=timestamp.isoformat()
        )

    metadata = found.groupdict()

    return FilePathData(
        date=metadata["prefix"],
        area_type=metadata["area_type"],
        category=metadata["category"],
        subcategory=metadata["subcategory"],
        timestamp=metadata["timestamp"]
    )


def category_label(parsed_filepath: FilePathData) -> str:
    category = parsed_filepath['category'] or "main"
    area_type = parsed_filepath.get('area_type')

    category_key = (category, area_type if not area_type else area_type.upper())

    if subcategory := parsed_filepath.get('subcategory'):
        process_name = category2process_name[category_key][subcategory]
    else:
        process_name = category2process_name[category_key]

    return process_name
