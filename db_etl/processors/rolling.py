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
import logging

# 3rd party:
from pandas import DataFrame, to_datetime
from numpy import NaN

# Internal: 
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger
    from utilities.latest_data import get_latest_csv

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'change_by_sum'
]


def col2datetime(d, col, format):
    d.loc[:, col] = to_datetime(d.loc[:, col], format=format)
    return d


def datetime2str(d, col, format):
    d.loc[:, col] = d.loc[:, col].map(lambda x: x.strftime(format))
    return d


def get_directions(d, col):
    d.loc[(d[col] < 0), col] = -1
    d.loc[(d[col] > 0), col] = 1
    d.loc[:, col] = d.loc[:, col].replace({-1: "DOWN", 0: "SAME", 1: "UP"})
    return d


def replace_all_zero(d):
    if d.sum() == 0:
        d[:] = NaN

    return d


def calculate_percentage_change(d):
    numerator = d.iloc[7]
    denominator = d.iloc[0]

    if numerator == 0 and denominator > 0:
        return -100

    fraction = (numerator / (denominator or 1)) - 1

    if fraction == -1:
        return 0

    return fraction * 100


@func_logger("change and direction by rolling sum calculation")
def change_by_sum(data: DataFrame, metrics, min_sum_allowed=None, min_sum_sub=None) -> DataFrame:
    """

    Parameters
    ----------
    data
    metrics
    min_sum_allowed
    min_sum_sub

    All values in rolling sum that are smaller than ``min_sum_allowed`` are substituted
    with ``min_sum_sub``. The latter is expected to be smaller than the former to prevent
    conflicts. At the end of the process, all calculated columns carrying ``min_sum_sub``,
    including the metric column, are substituted with ``NaN`` - .

    Returns
    -------

    """
    metrics = set(metrics).intersection(data.columns)

    data.sort_values(
        ["areaType", "areaCode", "date"],
        ascending=[True, True, True],
        inplace=True
    )

    logging.info(">> Starting to calculate the rolling metrics for")

    date_fmt = "%Y-%m-%d"
    date = "date"
    unique_loc_qualifiers = ["areaType", "areaCode"]
    unique_record_qualifiers = [*unique_loc_qualifiers, date]

    for col_name in metrics:
        rolling_sum_cols = [*unique_record_qualifiers, col_name]

        rolling_sum = f"{col_name}RollingSum"
        change = f"{col_name}Change"
        direction = f"{col_name}Direction"
        change_percentage = f"{col_name}ChangePercentage"

        # Local test
        # col_names.extend([col_name, rolling_sum, change, direction, change_percentage])

        logging.info(f"\t{col_name}")

        d = data.loc[:, rolling_sum_cols]
        d.loc[:, col_name] = d.loc[:, col_name].astype(float)

        if rolling_sum not in data.columns:
            df_rsum = (
                d
                .loc[:, rolling_sum_cols]
                .pipe(col2datetime, col=date, format=date_fmt)
                .groupby(unique_loc_qualifiers)
                .rolling(7, on=date)
                .sum()
                .rename(columns={col_name: rolling_sum})
                .reset_index()
                .loc[:, [*unique_record_qualifiers, rolling_sum]]
                .pipe(datetime2str, col=date, format=date_fmt)
                .set_index(unique_record_qualifiers)
            )
            logging.info("\t\tCalculated rolling sum")

            try:
                data.date = data.date.map(lambda x: x.strftime(date_fmt))
            except AttributeError:
                # Already string
                pass

            if min_sum_allowed is not None:
                df_rsum.loc[df_rsum[rolling_sum] < min_sum_allowed, rolling_sum] = min_sum_sub

            data = (
                data
                .set_index(unique_record_qualifiers)
                .join(df_rsum, on=unique_record_qualifiers)
                .reset_index()
            )
            logging.info("\t\tJoined rolling sum to dataset")

        data.loc[:, rolling_sum] = (
            data
            .groupby(unique_loc_qualifiers)[rolling_sum]
            .apply(replace_all_zero)
        )
        logging.info(f"\t\tGrouped data by {unique_loc_qualifiers}")

        df_tmp = data.loc[:, [*unique_record_qualifiers, rolling_sum]]

        df_tmp = df_tmp.assign(**{
            change: (
                df_tmp
                .pipe(col2datetime, col=date, format=date_fmt)
                .loc[:, [*unique_loc_qualifiers, rolling_sum]]
                .groupby(unique_loc_qualifiers)
                .diff(periods=7)
            ),
            direction: (
                df_tmp
                .pipe(col2datetime, col=date, format=date_fmt)
                .loc[:, [*unique_loc_qualifiers, rolling_sum]]
                .groupby(unique_loc_qualifiers)
                .diff(periods=7)
                .pipe(get_directions, col=rolling_sum)
            )
        })
        logging.info("\t\tCalculated rolling change (diff)")

        percentage_value = (
            df_tmp
            .pipe(col2datetime, col=date, format=date_fmt)
            .loc[:, [*unique_record_qualifiers, rolling_sum]]
            .groupby(unique_loc_qualifiers)
            .rolling(window=8, on=date)[rolling_sum]
            .apply(calculate_percentage_change)
            .round(1)
            .to_frame(change_percentage)
        )
        logging.info("\t\tCalculated percentage change")

        df_tmp = (
            df_tmp
            .join(percentage_value, on=unique_record_qualifiers)
            .pipe(datetime2str, col=date, format=date_fmt)
            .set_index(unique_record_qualifiers)
            .loc[:, [change, direction, change_percentage]]
        )
        logging.info("\t\tJoined percentage to other rolling figures")

        data = (
            data
            .join(df_tmp, on=unique_record_qualifiers)
            .reset_index(drop=True)
        )
        logging.info("\t\tJoined rolling figures to main dataset")

        data.loc[
            data.loc[:, col_name].isnull(),
            [rolling_sum, change, direction, change_percentage]
        ] = NaN

        logging.info("\t\tFinalised the data")

        if min_sum_allowed is not None:
            data.loc[
                data[rolling_sum] == min_sum_sub,
                [rolling_sum, change, direction, change_percentage, col_name]
            ] = NaN

    return data


if __name__ == '__main__':
    from sys import stdout
    from pandas import read_csv

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    # ----

    metrs = [
        # 'newCasesBySpecimenDate',
        # 'newCasesByPublishDate',
        # 'newAdmissions',
        # 'newDeaths28DaysByPublishDate',
        # 'newPCRTestsByPublishDate'
        "newCasesBySpecimenDate",
        "newCasesByPublishDate",
        "newDeathsByDeathDate",
        "newDeaths28DaysByDeathDate",
        "newDeaths60DaysByDeathDate",
        "newAdmissions"
    ]

    data_m = read_csv(
        "/Users/pouria/Downloads/processed_20201126-1503.csv",
        usecols=['areaName', 'areaType', 'areaCode', 'date', *metrs]
    )
    dft = change_by_sum(data_m, metrs)

    dft.loc[dft.areaName == "Haringey", :].to_string("test.txt")
