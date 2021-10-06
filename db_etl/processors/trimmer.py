#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       16 Dec 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime, timedelta

# 3rd party:
from pandas import DataFrame

# Internal:
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'trim_end'
]


@func_logger("trim end")
def trim_end(d: DataFrame, metrics, days_to_trim):
    metrics = set(metrics).intersection(d.columns)

    # d.date = d.date.map(lambda x: x.strftime("%Y-%m-%d"))
    max_date = datetime.strptime(d.date.max(), "%Y-%m-%d")
    max_date -= timedelta(days=days_to_trim)
    max_date = max_date.strftime("%Y-%m-%d")

    d.loc[:, metrics] = d.loc[:, metrics].where(d.date <= max_date, None)

    return d
