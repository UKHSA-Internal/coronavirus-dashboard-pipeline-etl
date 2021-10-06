#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       26 Jun 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from asyncio import sleep, Lock, get_event_loop
from json import loads

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.database import CosmosDB, Collection
except ImportError:
    from storage import StorageClient
    from database import CosmosDB, Collection

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'upload_from_file'
]


async def upsert(container, content):
    attempt = 0
    dt = {
        key: value
        for key, value in content.items()
        if value is not None
    }

    while attempt < 10:
        try:
            container.upsert(dt)
            return None
        except Exception as e:
            logging.warning(e)
            attempt += 1
            await sleep(attempt * 2)


async def download_file(container: str, path: str, lock: Lock) -> bytes:
    logging.info(f"> Downloading data from '{container}/{path}'")

    with StorageClient(container=container, path=path) as client:
        data = client.download()

    logging.info(f"> Download complete")

    return data.readall()


async def get_data(container: str, file_path: str, lock: Lock):
    blob = await download_file(container, file_path, lock)

    logging.info(f"> Parsing JSON data")
    data = loads(blob.decode())
    logging.info(f"> Total of {len(data)} was extracted from '{file_path}'")

    for document in data:
        yield document


async def upload_from_file(container, filepath):
    logging.info(f"--- Starting the process to upload data from '{filepath}'")

    loop = get_event_loop()
    lock = Lock()

    db_container = CosmosDB(Collection.DATA, writer=True)

    async for document in get_data(container, filepath, lock):
        loop.create_task(upsert(db_container, document))
