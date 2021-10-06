#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from itertools import product

# 3rd party:
from pandas import (
    DataFrame, to_datetime, date_range,
    unique, MultiIndex, concat
)

# Internal: 
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'homogenise_dates',
    'homogenise_demographics_dates'
]


@func_logger("homogenisation")
def homogenise_dates(d: DataFrame):
    """

    Parameters
    ----------
    d

    Returns
    -------

    """
    d.date = to_datetime(d.date, format="%Y-%m-%d")

    col_names = d.columns

    date = date_range(
        start=to_datetime(d.date).min(),
        end=to_datetime(d.date).max()
    )

    dt_time_list = list()

    for area_type in unique(d.areaType):
        values = product(
            [area_type],
            unique(d.loc[d.areaType == area_type, "areaCode"]),
            date
        )

        d_date = DataFrame(
                columns=["value"],
                index=MultiIndex.from_tuples(
                    tuples=list(values),
                    names=["areaType", "areaCode", "date"]
                )
        )
        dt_time_list.append(d_date)

    dt_time = concat(dt_time_list)
    dt_time.reset_index(inplace=True)

    d = d.merge(dt_time, how='outer', on=['areaType', 'areaCode', 'date'])

    d.sort_values(
        ["date", "areaType", "areaCode"],
        ascending=[True, True, False],
        inplace=True
    )

    return d.loc[:, col_names]


def homogenise_demographics_dates(d: DataFrame):
    """

    Parameters
    ----------
    d

    Returns
    -------

    """
    d.date = to_datetime(d.date, format="%Y-%m-%d")

    col_names = d.columns

    date = date_range(
        start=to_datetime(d.date).min(),
        end=to_datetime(d.date).max()
    )

    dt_time_list = list()

    age = d.age.unique()

    for area_type in unique(d.areaType):
        values = product(
            [area_type],
            unique(d.loc[d.areaType == area_type, "areaCode"]),
            date,
            age
        )

        d_date = DataFrame(
            columns=["value"],
            index=MultiIndex.from_tuples(
                tuples=list(values),
                names=["areaType", "areaCode", "date", "age"]
            )
        )
        dt_time_list.append(d_date)

    dt_time = concat(dt_time_list)
    dt_time.reset_index(inplace=True)

    d = d.merge(dt_time, how='outer', on=['areaType', 'areaCode', 'date', 'age'])

    d.sort_values(
        ["date", "areaType", "areaCode", "age"],
        ascending=[True, True, False, True],
        inplace=True
    )

    return d.loc[:, col_names]