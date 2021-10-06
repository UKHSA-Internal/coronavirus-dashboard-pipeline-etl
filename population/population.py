#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from json import loads
from typing import NamedTuple

# 3rd party:
from pandas import DataFrame

# Internal:
try:
    from __app__.utilities import func_logger
    from __app__.generic_types import PopulationData
except ImportError:
    from utilities import func_logger
    from utilities.generic_types import PopulationData

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_population_data'
]


@func_logger("population data processor")
def get_population_data(data) -> PopulationData:
    """
    Gets the population data from ``POPULATION_DATA_URL``, and
    turns it into a data frame indexed using the keys (area codes).

    The data is expected to be a JSON in the following format:

        {
            "#E01234": 1234
        }

    Returns
    -------
    PopulationData
        With indices representing the area code and one column: Population.
    """
    parsed = loads(data)

    result = PopulationData(
        general=DataFrame.from_dict(
            parsed['general'],
            orient='index',
            columns=['population']
        ),
        ageSexBroadBreakdown=parsed['ageSexBroadBreakdown'],
        ageSex5YearBreakdown=parsed['ageSex5YearBreakdown']
    )

    return result

