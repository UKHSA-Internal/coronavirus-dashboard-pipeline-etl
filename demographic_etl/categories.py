#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       09 Dec 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple, List

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class DemographicsCategoryItem(NamedTuple):
    name: str
    directory: str
    metric_name: str
    output_metric: str
    output_columns: List[str]


class DemographicsCategoryBase(NamedTuple):
    specimen_date_cases: DemographicsCategoryItem
    publish_date_cases: DemographicsCategoryItem
    death28d_date_deaths: DemographicsCategoryItem


DemographicsCategory = DemographicsCategoryBase(
    specimen_date_cases=DemographicsCategoryItem(
        name="specimen_date",
        directory="specimen_date_cases",
        metric_name="newCasesBySpecimenDate",
        output_metric="newCasesBySpecimenDateAgeDemographics",
        output_columns=[
            "age",
            "newCasesBySpecimenDate",
            "newCasesBySpecimenDateRollingSum",
            "newCasesBySpecimenDateRollingRate"
        ]
    ),
    publish_date_cases=DemographicsCategoryItem(
        name="publish_date",
        directory="publish_date_cases",
        metric_name="newCasesByPublishDate",
        output_metric="newCasesByPublishDateAgeDemographics",
        output_columns=[
            "age",
            "newCasesByPublishDate",
        ]
    ),
    death28d_date_deaths=DemographicsCategoryItem(
        name="deaths_28days_death_date",
        directory="deaths_28days_death_date",
        metric_name="newDeaths28DaysByDeathDate",
        output_metric="newDeaths28DaysByDeathDateAgeDemographics",
        output_columns=[
            "age",
            "newDeaths28DaysByDeathDate",
            "newDeaths28DaysByDeathDateRollingSum",
            "newDeaths28DaysByDeathDateRollingRate"
        ]
    )
)

