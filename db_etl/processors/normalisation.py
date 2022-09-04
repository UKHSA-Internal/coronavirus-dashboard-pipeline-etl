#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import Iterable
from itertools import product

# 3rd party:
from pandas import DataFrame, unique

# Internal: 
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'normalise_records',
    'normalise_demographics_records'
]


@func_logger("normalisation")
def normalise_records(d: DataFrame, zero_filled: Iterable[str] = tuple(),
                      cumulative: Iterable[str] = tuple(),
                      reset_index: bool = False) -> DataFrame:
    """

    Parameters
    ----------
    d
    zero_filled
    cumulative
    reset_index

    Returns
    -------

    """
    zero_filled = set(zero_filled).intersection(d.columns)
    cumulative = set(cumulative).intersection(d.columns)

    if not reset_index:
        d.sort_values(
            ["areaType", "areaCode", "date"],
            inplace=True
        )
    else:
        d = (
            d
            .reset_index()
            .sort_values(["areaType", "areaCode", "date"])
        )

    for col in zero_filled:
        for areaCode in unique(d.areaCode):
            dm = d.loc[d.areaCode == areaCode, [col, 'date']]
            indices = (
                (d.areaCode == areaCode) &
                (d.date < dm.dropna(axis=0).date.max()) &
                (d.date >= dm.dropna(axis=0).date.min())
            )
            d.loc[indices, col] = d.loc[indices, col].fillna(0)

    # Area names are scattered around - we cannot use
    # normal `fillna` to fill them.
    if "areaName" in d.columns:
        for areaCode in unique(d.areaCode):
            area_name = unique(d.loc[d.areaCode == areaCode, "areaName"].dropna().values)[0]
            d.loc[d.areaCode == areaCode, 'areaName'] = area_name

    for col in cumulative:
        for areaCode in unique(d.areaCode):
            dm = d.loc[d.areaCode == areaCode, [col, 'date']]
            indices = (
                (d.areaCode == areaCode) &
                (d.date < dm.dropna(axis=0).date.max()) &
                (d.date >= dm.dropna(axis=0).date.min())
            )

            d.loc[indices, col] = d.loc[indices, col].fillna(method="ffill")

    d.date = d.date.map(lambda x: x.strftime("%Y-%m-%d"))

    if "areaName" in d.columns:
        d = d.assign(areaNameLower=d.areaName.str.lower())

    return d


def normalise_demographics_records(d: DataFrame,
                                   nesting_param: str,
                                   base_metrics: Iterable[str],
                                   zero_filled: Iterable[str] = tuple(),
                                   cumulative: Iterable[str] = tuple()) -> DataFrame:
    """

    Parameters
    ----------
    d
    zero_filled
    cumulative

    Returns
    -------

    """
    zero_filled = set(zero_filled).intersection(d.columns)
    cumulative = set(cumulative).intersection(d.columns)

    d = d.reset_index().sort_values(base_metrics)

    d.loc[:, zero_filled] = (
        d
        .loc[:, zero_filled]
        .where(d.loc[:, zero_filled].notnull(), 0)
    )

    # Area names are scattered around - we cannot use
    # normal `fillna` to fill them.
    if "areaName" in d.columns:
        for areaCode in d.areaCode.dropna().unique():
            area_name = unique(d.loc[d.areaCode == areaCode, "areaName"].dropna().values)[0]
            d.loc[d.areaCode == areaCode, 'areaName'] = area_name

    # All cumulative metrics should have the same starting
    # point across different bands.
    d.loc[d.date == d.date.min(), cumulative] = (
        d
        .loc[d.date == d.date.min(), cumulative]
        .where(d.loc[d.date == d.date.min(), cumulative].notnull(), 0)
    )

    for col, areaCode, nested_value in product(cumulative, d.areaCode.unique(), d[nesting_param].unique()):
        dm = d.loc[((d.areaCode == areaCode) & (d[nesting_param] == nested_value)), [col, 'date']]

        indices = (
                (d.areaCode == areaCode) &
                (d[nesting_param] == nested_value) &
                (d.date < dm.dropna(axis=0).date.max()) &
                (d.date >= dm.dropna(axis=0).date.min())
        )

        d.loc[indices, col] = d.loc[indices, col].fillna(method="ffill")

    d.date = d.date.map(lambda x: x.strftime("%Y-%m-%d"))

    if "areaName" in d.columns:
        d = d.assign(areaNameLower=d.areaName.str.lower())

    return d
