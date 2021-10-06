#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from re import compile as re_compile, IGNORECASE
from os.path import join as join_path
from asyncio import Lock, get_event_loop
from typing import NoReturn, Awaitable, Callable
from http import HTTPStatus
from datetime import datetime

# 3rd party:
from azure.functions import TimerRequest
from pytz import timezone

# Internal:
try:
    # Imports within the Function
    from __app__.github_utils import process_github_files
    from __app__.github_utils.types import CallbackType, FileFetcherType
    from __app__.storage import StorageClient
    from __app__ import processor_settings
except ImportError:
    # Local alternative
    from github_utils import process_github_files
    from github_utils.types import CallbackType, FileFetcherType
    from storage import StorageClient
    import processor_settings

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    "process_modals"
]


STORAGE_CONTAINER = "publicdata"
STORAGE_PATH = "assets"

REPOSITORY_NAME = "coronavirus-dashboard-metadata"
REPOSITORY_BASE = f"{REPOSITORY_NAME}-{processor_settings.GITHUB_BRANCH}"
REPOSITORY_BASE_PATH = join_path(REPOSITORY_BASE, "modals")
REPOSITORY_ARCHIVE_URL = f"{REPOSITORY_NAME}/archive/{processor_settings.GITHUB_BRANCH}.zip"

EXCLUDED_ITEMS = [
    "README.md",
    "LICENSE",
    ".github_utils",
    ".gitignore"
]

timezone_LN = timezone("Europe/London")
timestamp = datetime.now(timezone_LN)

UPDATE_TIMESTAMP = f"""

---

Last updated on { timestamp.strftime("%A, %d %B %Y") } at \
{ timestamp.strftime("%I:%M%p") }

"""


async def substitute(raw_data: Awaitable[str],
                     get_file_data: Callable[[str, str], Awaitable[str]],
                     base_path: str) -> str:
    """
    Substitutes the placeholders in the files using the
    contents of the file specified in te placeholder.

    Parameters
    ----------
    raw_data: Awaitable[str]
        Contents of the file whose placeholders are to be
        substituted.

    get_file_data: Callable[[str, str], Awaitable[str]]

    base_path: str

    Returns
    -------
    str
    """
    pattern = re_compile(
        r"{inc:(?P<relative_path>[a-z0-9./_\-]+)\|(?P<category>[a-z0-9_\-]+)}",
        flags=IGNORECASE
    )

    data = await raw_data

    # Substitution process
    # Note: Do not use Walrus operators as the
    #       function runs on Python 3.7
    while True:
        match = pattern.search(data)

        if match is None:
            break

        found = match.group(0)
        relative_path = match.group("relative_path")

        replacement = await get_file_data(relative_path, base_path)

        data = data.replace(found, replacement)

    return data


async def process_and_upload_data(path: str, get_file_data: FileFetcherType,
                                  container: str, base_path: str) -> NoReturn:
    """
    Uploads processed files to the storage using the correct
    caching and ``content-type`` specs.

    Parameters
    ----------
    path: str
        Path (within the storage container) in which the
        file is to be stored.

    get_file_data: FileFetcherType

    base_path: str

    container: str
        Storage container in which the file is to be stored.

    Returns
    -------
    NoReturn
    """
    upload_path = path.lstrip(REPOSITORY_BASE).strip(processor_settings.URL_SEPARATOR)
    blob_path = str.join(processor_settings.URL_SEPARATOR, [STORAGE_PATH, upload_path])

    raw_data = get_file_data(path, base_path)
    data = await substitute(raw_data, get_file_data, base_path)

    # Uploading the data
    with StorageClient(container=container, path=blob_path,
                       content_type="text/markdown; charset=utf-8") as client:
        async with Lock():
            client.upload(data=data + UPDATE_TIMESTAMP)


process_and_upload_data: CallbackType = process_and_upload_data


async def process_modals(timer: TimerRequest) -> NoReturn:
    """
    Modal processor (main entry point).

    Processes markdown files and substitutes the placeholders.

    Parameters
    ----------
    req: HttpRequest
        HTTP trigger request (web hook).

    Returns
    -------
    HttpResponse
        Response to the HTTP request (200 if successful, otherwise 500).
    """
    logging.info(f"--- Web hook has triggered the function. Starting the process...")

    event_loop = get_event_loop()

    try:
        task = process_github_files(
            repository_url=REPOSITORY_ARCHIVE_URL,
            callback=process_and_upload_data,
            excluded=EXCLUDED_ITEMS,
            base_path=REPOSITORY_BASE_PATH,
            container=STORAGE_CONTAINER
        )

        event_loop.create_task(task)

    except Exception as err:
        logging.exception(err)


if __name__ == "__main__":
    # Local test
    from asyncio import gather

    loop = get_event_loop()

    proc = process_modals("")
    loop.run_until_complete(gather(proc))
