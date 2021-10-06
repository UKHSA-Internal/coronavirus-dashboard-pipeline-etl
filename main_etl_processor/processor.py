#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv

# 3rd party:

# Internal:
try:
    from __app__.db_etl.etl import run
    from __app__.utilities.generic_types import RawDataPayload
except ImportError:
    from db_etl.etl import run
    from utilities.generic_types import RawDataPayload

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


DEBUG = getenv("DEBUG", False)
ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")


def main(areaData):
    result = run(areaData)

    return result
