#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       21 Oct 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv
# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'SITE_URL',
    'PUBLICDATA_PARTITION'
]


ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")

SITE_URL = str()

if ENVIRONMENT == "STAGING":
    SITE_URL = "https://coronavirus-staging.data.gov.uk"
    PUBLICDATA_PARTITION = "releaseTimestamp"
elif ENVIRONMENT == "PRODUCTION":
    SITE_URL = "https://coronavirus.data.gov.uk"
    PUBLICDATA_PARTITION = "releaseDate"
else:
    SITE_URL = "/"
    PUBLICDATA_PARTITION = "releaseDate"
