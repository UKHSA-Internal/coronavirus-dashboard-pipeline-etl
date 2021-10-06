#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from hashlib import blake2s

# 3rd party:
from pandas import DataFrame

# Internal:
try:
    from __app__.utilities import func_logger
except ImportError:
    from utilities import func_logger

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_row_hash'
]


@func_logger("hash calculation")
def generate_row_hash(d: DataFrame, hash_only=False, date=None) -> DataFrame:
    """

    Parameters
    ----------
    d
    hash_only
    date

    Returns
    -------

    """
    hash_cols = [
        "date",
        "areaType",
        "areaCode",
    ]

    if date is None:
        date = d.date.max()

    # Create hash
    hash_key = (
        d.loc[:, hash_cols]
         .astype(str)
         .sum(axis=1)
         .apply(str.encode)
         .apply(
            lambda x: blake2s(x, key=date.encode(), digest_size=32).hexdigest()
         )
    )

    if hash_only:
        return hash_key

    column_names = d.columns

    data = d.assign(
        hash=hash_key,
        seriesDate=date,
        id=hash_key
    ).loc[:, ['id', 'hash', 'seriesDate', *list(column_names)]]

    return data
