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


def homogenise_demographics_dates(
        d: DataFrame,
        base_metrics,
        nesting_param,
        frequency,
):

    d.date = to_datetime(d.date, format="%Y-%m-%d")

    col_names = d.columns

    date = date_range(
        start=to_datetime(d.date).min(),
        end=to_datetime(d.date).max(),
        freq=frequency
    )

    dt_time_list = list()

    unique_nesting_param_values = d[nesting_param].unique()

    for area_type in unique(d.areaType):
        values = product(
            [area_type],
            unique(d.loc[d.areaType == area_type, "areaCode"]),
            date,
            unique_nesting_param_values
        )

        d_date = DataFrame(
            columns=["value"],
            index=MultiIndex.from_tuples(
                tuples=list(values),
                names=base_metrics
            )
        )
        dt_time_list.append(d_date)

    dt_time = concat(dt_time_list)
    dt_time.reset_index(inplace=True)

    d = d.merge(dt_time, how='outer', on=base_metrics)

    d.sort_values(
        base_metrics,
        ascending=[True, True, False, True],
        inplace=True
    )

    return d.loc[:, col_names]