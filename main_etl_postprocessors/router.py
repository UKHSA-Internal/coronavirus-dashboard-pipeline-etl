#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       10 Sep 2021
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import TypedDict
import logging

# 3rd party:

# Internal:
from .private_report import process as generate_private_report

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
    'Payload'
]


class Payload(TypedDict):
    timestamp: str
    original_path: str
    environment: str


def main(payload: Payload):
    logging.info(f"Main postprocess launched with {payload}")

    # Functions to be executed.
    processes = [
        generate_private_report,
    ]

    for func in processes:
        try:
            logging.info(f"Executing main postprocessor '{func.__name__}'")
            func(payload)
        except Exception as err:
            logging.info(
                f"FAILED: main postprocessor '{func.__name__}' failed: \n"
                f"{payload} \n"
                f"{err}"
            )

    return f"DONE: {payload['timestamp']}"
