#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:
from pandas import DataFrame
from numpy import NaN

# Internal: 
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'calculate_by_adjacent_column',
    'calculate_pair_summations'
]


@func_logger("calculation by adjacent column")
def calculate_by_adjacent_column(data: DataFrame, **columns_dict) -> DataFrame:
    """
    Calculates cumulative tests by publish date for the latest
    date, where one is not already available.

    Parameters
    ----------
    data: DataFrame
        Full data table containing with the following columns:

            - areaType
            - areaCode
            - date
            - cumNegativesBySpecimenDate
            - cumCasesBySpecimenDate
            - cumPeopleTestedByPublishDate

    columns_dict: dict
        Key-value pairs of columns to be calculated.

    Returns
    -------
    DataFrame
        Same data table as `data`, with some of the
        missing `cumPeopleTestedByPublishDate` added.
    """
    columns_dict = {
        key: value
        for key, value in columns_dict.items()
        if value in data.columns
    }

    for target, source in columns_dict.items():
        max_by_loc = (
            data
            .loc[:, ["areaType",  "areaCode", source]]
            .dropna(axis=0)
            .groupby(["areaType", "areaCode"])
            .max()
            .rename(columns={source: "value"})
        )

        if target not in data:
            data[target] = NaN

        indices = (
            # Where `cumPeopleTestedByPublishDate` is NaN.
            (data[target].isna()) &
            # Only the latest date.
            (data.date == data.date.max())
        )

        joined_data = (
            data
            .loc[
                indices,
                # Only take the columns needs (for speed and memory efficiency).
                ["areaType", "areaCode", target]
            ]
            # Left join the two datasets on (`areaType`, `areaCode`).
            # Note: The join only occurs where the two identifier in `this` match
            #       their counterparts in `other`.
            .join(
                other=max_by_loc,
                on=["areaType", "areaCode"],
                how='left'
            )
        )

        # Incorporating the calculated values for
        # the `cumPeopleTestedByPublishDate` column
        # into the original dataset.
        data.loc[joined_data.index, [target]] = joined_data.value

    return data


@func_logger("pair summation calculator")
def calculate_pair_summations(data: DataFrame, **pairs) -> DataFrame:
    """
    Calculates sum of pairs.

    Parameters
    ----------
    data: DataFrame
        Full data table.

    pairs: dict

    Returns
    -------
    DataFrame
        Same data table as `data`, with a columns containing the sum of
        pairs in ``pairs``.
    """
    pairs = {
        key: value
        for key, value in pairs.items()
        if all(val in data.columns for val in value)
    }

    data = data.assign(**{
        key: (
            data
            .loc[:, [*value]]
            .sum(axis=1, min_count=2)
        )
        for key, value in pairs.items()
    })

    return data
