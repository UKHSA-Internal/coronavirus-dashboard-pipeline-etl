#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import Optional, Any, Dict, NoReturn
from io import BytesIO

# 3rd party:
from pandas import DataFrame

# Internal:
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'save_chunk_feather',
    'upload_chunk_feather'
]


UPLOAD_KWS = dict(
    content_type="application/octet-stream",
    compressed=False,
    tier='Cool'
)


def save_chunk_feather(*, data: DataFrame, dir_path: str, filename: str,
                       **kwargs: Optional[Dict[str, Any]]) -> NoReturn:
    """
    Requires keyword arguments.

    Parameters
    ----------
    data: DataFrame

    dir_path: str
        Storage directory path.

    filename: str
        Name of the file to be stored.

    kwargs: Optional[Dict[str, Any]]
        For signature compatibility with other functions in the module.

    Returns
    -------
    NoReturn
    """
    data.reset_index(drop=True).to_feather(path=f"{dir_path}/{filename}")


def upload_chunk_feather(*, data: DataFrame, container: str, dir_path: str,
                         filename: str) -> NoReturn:
    """
    Requires keyword arguments.

    Parameters
    ----------
    data: DataFrame

    container: str
        Storage container name.

    dir_path: str
        Storage directory path.

    filename: str
        Name of the file to be stored.

    Returns
    -------
    NoReturn
    """
    file_obj = BytesIO()
    data.reset_index(drop=True).to_feather(file_obj)
    file_obj.seek(0)

    bin_data = file_obj.read()

    with StorageClient(container=container, path=f"{dir_path}/{filename}", **UPLOAD_KWS) as cli:
        cli.upload(bin_data)
