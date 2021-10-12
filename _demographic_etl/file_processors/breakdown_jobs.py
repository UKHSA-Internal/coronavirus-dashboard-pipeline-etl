#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       30 Oct 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from asyncio import get_event_loop

# 3rd party:

# Internal:
try:
    from .specimen_date import create_specimen_date_dataset
    from .publish_date import create_publish_date_dataset
except ImportError:
    from demographic_etl.file_processors.specimen_date import create_specimen_date_dataset
    from demographic_etl.file_processors.publish_date import create_publish_date_dataset

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'create_breakdown_datasets'
]


async def create_breakdown_datasets():
    event_loop = get_event_loop()

    tasks = [
        create_specimen_date_dataset,
        create_publish_date_dataset
    ]

    for task in tasks:
        event_loop.create_task(task())
