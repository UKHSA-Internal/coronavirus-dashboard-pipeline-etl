#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from json import dumps
from typing import Iterable, Optional

# 3rd party:
from pandas import DataFrame, notnull, isna

# Internal:
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'produce_json'
]


@func_logger("JSON construction")
def produce_json(data: DataFrame, value_columns: Iterable[str],
                 *non_integer_columns: Optional[str]) -> str:
    """
    Produces a JSON output from the structured data.

    Parameters
    ----------
    data: DataFrame
        Structured data that is both sorted and filtered to include only
        the data that is needed in the output.

    value_columns: Iterable[str]
        Columns containing a numeric value.

    *non_integer_columns: str
        Name of non-integer columns.

    Returns
    -------
    str
        JSON output as a string object.
    """
    data = data.where(notnull(data), None)
    group_values = data.to_dict(orient='records')

    logging.info(">> Converting data types")

    integer_columns = set(value_columns) - set(non_integer_columns)

    # Values default to float because of missing numbers.
    # There's no easy / clean way to convert them onto int.
    for index, value in enumerate(group_values):
        # NOTE: The `value` is an alias (link) to a item in `group_values`,
        # and any alterations to it will be implemented in the original
        # item in `group_values`.
        for col in integer_columns:
            if not isna(value[col]):
                value[col] = int(value[col])

    logging.info(">> Dumping JSON")
    json_file = dumps(
        group_values,
        allow_nan=False,
        separators=(',', ':')
    )

    return json_file
