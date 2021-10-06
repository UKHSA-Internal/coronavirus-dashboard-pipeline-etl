"""
Coronavirus (COVID-19) in the UK - Dashboard service
====================================================

Consumer data pipeline ETL
--------------------------

ETL service to create consumer-ready CSV and JSON files for download.

The service is dispatched by an event that is triggered every time
a new data file is deployed to the ``downloads`` blob storage container.

Data are identical to the original source, but enjoys a different structure.

.. Note::
    There are missing values in the data. The approach is to leave them
    as blank in the CSV file, and assign a ``null`` value in JSON to
    ensure a consistent structure.


Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       30 May 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.5.4"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from .etl import *
from .db_uploader import upload_from_file, combine_and_upload_from_file
