#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       05 Nov 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

headline_metrics = {
    'cases': {
        "metric": 'newCasesBySpecimenDate',
        "caption": "Cases",
    },
    'deaths': {
        "metric": 'newDeaths28DaysBySpecimenDate',
        "caption": "Deaths",
    },
    'healthcare': {
        "metric": 'newAdmissions',
        "caption": "Healthcare",
    },
    'testing': {
        "metric": 'newVirusTestsByPublishDate',
        "caption": "Testing",
    }
}

vaccinations = [
    "cumVaccinationFirstDoseUptakeByPublishDatePercentage",
    "cumVaccinationSecondDoseUptakeByPublishDatePercentage"
]
