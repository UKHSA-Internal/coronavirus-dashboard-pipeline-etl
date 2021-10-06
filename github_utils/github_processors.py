#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from zipfile import ZipFile
from io import BytesIO
from tempfile import gettempdir
from os.path import split as split_path, join as join_path, isfile
from asyncio import Lock, get_event_loop
from typing import AsyncIterator, NoReturn, Optional, Iterable
from http import HTTPStatus

# 3rd party:
from requests import get as get_request

# Internal:
from .types import FileFetcherType, CallbackType

try:
    from __app__ import settings
except ImportError:
    import processor_settings

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "get_file_data",
    "process_github_files",
    "get_fresh_data"
]

BASE_REPOSITORY = "https://github.com/publichealthengland/"

TEMP_DIR_PATH = gettempdir()


async def _get_file_data(relative_path: str, base_path: Optional[str] = "") -> str:
    """
    Get data from the temp directory.

    Parameters
    ----------
    relative_path: str
        Relative path from the base directory to the file.

    base_path: str
        Relative path to the base directory in the repository.

    .. Attention::
        This function only yields results if ``get_fresh_data`` has already
        been invoked; otherwise a ``FileNotFoundError`` will be raised.

    Returns
    -------
    str
        File data.
    """
    # Relative path (those extracted from the template), do not
    # include the `BASE_PATH`.
    if base_path and base_path not in relative_path:
        full_path = join_path(TEMP_DIR_PATH, base_path, relative_path)
    else:
        full_path = join_path(TEMP_DIR_PATH, relative_path)

    # Discard directories
    if not isfile(full_path):
        raise IsADirectoryError(
            "Expected a file path, received a directory "
            "path instead: '%s'" % full_path
        )

    # Reading the data from the temp directory.
    async with Lock():
        try:
            with open(full_path, mode="r") as file:
                return file.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File '{full_path}' does not exist. Have you already "
                f"invoked `get_fresh_data`?"
            )


get_file_data: FileFetcherType = _get_file_data


async def get_fresh_data(repository_url: str, excluded: Iterable) -> AsyncIterator[str]:
    """
    Retrieve a fresh batch of data from the repository.

    Parameters
    ----------
    repository_url: str
        URL for the repository (the zip file).

    excluded: Iterable

    Returns
    -------
    AsyncIterator[str]
        An async iterator of relative paths to the file.
    """
    url = BASE_REPOSITORY + repository_url.lstrip(processor_settings.URL_SEPARATOR)

    # Requesting the latest files from the repository.
    async with Lock():
        response = get_request(url=url)

        logging.info(
            f"> Download request completed with "
            f"status {response.status_code}: {repository_url}"
        )

    if response.status_code != HTTPStatus.OK:
        raise RuntimeError(f"Failed to download the data from {url}: {response.text}")

    # `ZipFile` only understands files.
    data_bin = BytesIO(response.content)

    async with Lock():
        with ZipFile(data_bin, mode="r") as zip_obj:
            paths = zip_obj.namelist()

            # Extracting the contents into the temp directory.
            zip_obj.extractall(TEMP_DIR_PATH)
        logging.info("> Successfully extracted and stored the data")

    for path in paths:
        _, filename = split_path(path)

        if any(map(lambda p: p in path, excluded)):
            continue

        full_path = join_path(TEMP_DIR_PATH, path)

        # Discard directories
        if not isfile(full_path):
            continue

        logging.info(f"> Processing file '{path}'")

        yield path


async def process_github_files(repository_url: str, callback: CallbackType,
                               excluded: Optional[Iterable] = tuple(),
                               *args, **kwargs) -> NoReturn:
    loop = get_event_loop()

    async for path in get_fresh_data(repository_url, excluded):
        future_task = callback(path, get_file_data, *args, **kwargs)
        loop.create_task(future_task)
