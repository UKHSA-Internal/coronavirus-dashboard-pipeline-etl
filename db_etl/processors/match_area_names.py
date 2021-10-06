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
from io import StringIO

# 3rd party:
from pandas import DataFrame, read_csv, MultiIndex

# Internal: 
try:
    from __app__.utilities import get_storage_file
    from __app__.utilities import func_logger
except ImportError:
    from utilities import get_storage_file
    from utilities import func_logger


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    'match_area_names'
]

AREA_TYPE_NAMES = {
    'nations': 'nation',
    'nhsTrusts': 'nhsTrust',
    'utlas': 'utla',
    'ltlas': 'ltla',
    'nhsRegions': 'nhsRegion',
    'regions': 'region',
    'uk': 'overview'
}

@func_logger("match area names")
def match_area_names(d: DataFrame, area_type_repls):
    ref_names_io = StringIO(get_storage_file("pipeline", "assets/geoglist.csv"))
    ref_names = read_csv(ref_names_io, usecols=["areaType", "areaCode", "areaName"])
    ref_names.replace(area_type_repls, inplace=True)
    ref_names = ref_names.loc[ref_names.areaCode.isin(d.areaCode), :]
    ref_names.set_index(["areaType", "areaCode"], inplace=True)
    # print(ref_names.areaType)
    d = d.drop(columns=['areaType', 'areaName'])
    print("post", d.shape)
    result = (
        d
        # .drop(columns=['areaName'])
        .join(ref_names, on=["areaType", "areaCode"], how="left")
    )

    return result


if __name__ == "__main__":
    df = read_csv(
        "/Users/pouria/Documents/Projects/coronavirus-data-etl/db_etl/test/v2/archive/processed_20201216-1545.csv",
        usecols=["areaCode", "areaType", "date", "areaName", "newCasesBySpecimenDate"],
        low_memory=False
    ).replace(AREA_TYPE_NAMES)

    print(df.shape)
    df = match_area_names(df, AREA_TYPE_NAMES)
    print(df.shape)
    print(df.tail(10).to_string())
