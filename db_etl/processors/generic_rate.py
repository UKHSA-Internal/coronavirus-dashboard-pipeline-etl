#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import Iterable
from datetime import datetime, timedelta

# 3rd party:
from pandas import DataFrame, to_datetime
from numpy import NaN

# Internal: 
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'calculate_rates',
    'calculate_age_rates'
]


RATE_PER_POPULATION_FACTOR = 100_000


@func_logger("rate calculation")
def calculate_rates(data: DataFrame, population_data: DataFrame,
                    rolling_rate: Iterable[str] = tuple(),
                    incidence_rate: Iterable[str] = tuple(),
                    rate_per_n: int = RATE_PER_POPULATION_FACTOR) -> DataFrame:
    """

    Parameters
    ----------
    data
    population_data
    rolling_rate
    incidence_rate
    rate_per_n

    Returns
    -------

    """
    data.sort_values(
        ["areaType", "areaCode", "date"],
        ascending=[True, True, True],
        inplace=True
    )

    rolling_rate = set(rolling_rate).intersection(data.columns)
    incidence_rate = set(incidence_rate).intersection(data.columns)

    max_date = datetime.strptime(data.date.max(), "%Y-%m-%d")
    latest_rate_date = (max_date - timedelta(days=5)).strftime("%Y-%m-%d")

    logging.info(">> Starting to calculate the rolling rate for")
    for col_name in rolling_rate:
        logging.info(f"\t\t{col_name}")

        rolling_sum_cols = [
            "date",
            "areaType",
            "areaCode",
            col_name
        ]

        d = data.loc[:, rolling_sum_cols]

        d.loc[:, col_name] = d.loc[:, col_name].astype(float)
        d.date = to_datetime(d.date)

        df_rolling_sum = (
            d
            .loc[:, rolling_sum_cols]
            .groupby(["areaType", "areaCode"])
            .rolling(7, on="date")
            .sum()
            .rename(columns={col_name: f"{col_name}RollingSum"})
            .reset_index(level=1)
        )

        dd_grouped = (
            df_rolling_sum
            .groupby(["areaType", "areaCode", "date"])
            .sum()
        )

        d = (
            d
            .join(dd_grouped, on=["areaType", "areaCode", "date"])
            .join(population_data.general, on=["areaCode"])
        )

        data = data.assign(**{
            f"{col_name}RollingSum": d[f"{col_name}RollingSum"],
            f"{col_name}RollingRate": (
                    d[f"{col_name}RollingSum"] / d.population * rate_per_n  # / 7 * 365.25 / d.population * rate_per_n
            ).round(1)
        })

        if "SpecimenDate" in col_name or "DeathDate" in col_name:
            data.loc[
                data.date > latest_rate_date,
                [f"{col_name}RollingSum", f"{col_name}RollingRate"]
            ] = NaN
        else:
            max_date = datetime.strptime(data.loc[:, [col_name, 'date']].dropna().date.max(), "%Y-%m-%d")
            data.loc[
                data.date > f"{max_date:%Y-%m-%d}",
                [f"{col_name}RollingSum", f"{col_name}RollingRate"]
            ] = NaN

    logging.info(">> Starting to calculate the incidence rate for")

    for col_name in incidence_rate:
        logging.info(f"\t\t {col_name}")
        d = (
            data
            .loc[:, ["areaType", "areaCode", "date", col_name]]
            .join(population_data.general, on=["areaCode"])
        )

        data = data.assign(**{
            f"{col_name}Rate": (
                (d[col_name] / d.population) * rate_per_n
            ).astype(float).round(1)
        })

    return data


# @func_logger("age rate calculation")
def calculate_age_rates(data: DataFrame, population_data: DataFrame,
                        max_date: datetime,
                        rolling_rate: Iterable[str] = tuple(),
                        rate_per_n: int = RATE_PER_POPULATION_FACTOR) -> DataFrame:
    """

    Parameters
    ----------
    data
    population_data
    rolling_rate
    max_date
    rate_per_n

    Returns
    -------

    """
    # print(data.head().to_string())
    data.sort_values(
        ["areaType", "areaCode", "date"],
        ascending=[True, True, True],
        inplace=True
    )

    logging.info(">> Starting to calculate the rolling rate for")
    for col_name in rolling_rate:
        logging.info(f"\t\t{col_name}")

        rolling_sum_cols = [
            "date",
            "areaType",
            "areaCode",
            "age",
            col_name
        ]

        d = data.loc[:, rolling_sum_cols]
        d.loc[:, col_name] = d.loc[:, col_name].astype(float)
        d.date = to_datetime(d.date)

        df_rolling_sum = (
            d
            .loc[:, rolling_sum_cols]
            .groupby(["areaType", "areaCode", "age"])
            .rolling(7, on="date")
            .sum()
            .rename(columns={col_name: f"{col_name}RollingSum"})
            .reset_index(level=1)
        )

        dd_grouped = (
            df_rolling_sum
            .groupby(["areaType", "areaCode", "date", "age"])
            .sum()
        )

        d = (
            d
            .join(dd_grouped, on=["areaType", "areaCode", "date", "age"])
            .join(population_data, on=["areaCode", "age"])
        )

        data = data.assign(**{
            f"{col_name}RollingSum": d[f"{col_name}RollingSum"],
            f"{col_name}RollingRate": (
                    d[f"{col_name}RollingSum"] / d.population * rate_per_n  # / 7 * 365.25 / d.population * rate_per_n
            ).round(1)
        })

        if "SpecimenDate" in col_name or "DeathDate" in col_name:
            data.loc[
                data.date > max_date,
                [f"{col_name}RollingSum", f"{col_name}RollingRate"]
            ] = NaN

    return data
