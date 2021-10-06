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
from typing import NamedTuple
import logging
from os import getenv
from os.path import split, join
from asyncio import sleep, Lock, get_event_loop
from json import loads

# 3rd party:
from azure.cosmos.cosmos_client import CosmosClient
from azure.functions import EventGridEvent
from azure.storage.blob import BlobClient

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main_worker'
]

DEBUG = False

STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
CONTAINER_NAME = getenv("StorageContainerName")
BLOB_DIR = "db_chunks"


class Credentials(NamedTuple):
    host = getenv("AzureCosmosHost")
    key = getenv("AzureCosmosWriterKey")
    db_name = getenv("AzureCosmosDBName")
    collection = getenv("AzureCosmosCollection")


async def upsert(container, content, lock: Lock):
    attempt = 0
    dt = {
        key: value
        for key, value in content.items()
        if value is not None
    }

    while attempt < 10:
        try:
            async with lock:
                container.upsert_item(body=dt)
            return None
        except Exception as e:
            logging.warning(e)
            attempt += 1
            await sleep(attempt)


async def download_file(container: str, path: str, lock: Lock) -> bytes:
    client = BlobClient.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name=container,
        blob_name=path
    )

    async with lock:
        data = client.download_blob()

    return data.readall()


async def get_data(filename: str, lock: Lock):
    logging.info(f"> Downloading data from { filename }")

    blob = await download_file(
        container=CONTAINER_NAME,
        path=join(BLOB_DIR, filename),
        lock=lock
    )

    logging.info(f"\t Download complete")

    logging.info(f"> Parsing JSON data")
    data = loads(blob.decode())
    logging.info(f"\t Total of { len(data) } was extracted from { filename }")

    for document in data:
        yield document


async def main_worker(event: EventGridEvent):
    logging.info(f"--- EventGrid has triggered the function. Starting the process")

    logging.info(f"> Parsing event data")
    event_json = event.get_json()
    blob_url = event_json["url"]

    _, filename = split(blob_url)

    logging.info(f"> Initialising DB client")
    client = CosmosClient(
        url=Credentials.host,
        credential={'masterKey': Credentials.key}
    )

    db = client.get_database_client(Credentials.db_name)
    container = db.get_container_client(Credentials.collection)

    logging.info(f"> Triggering the upsert process")

    loop = get_event_loop()
    lock = Lock()

    async for document in get_data(filename, lock):
        loop.create_task(upsert(container, document, lock))


if DEBUG and __name__ == "__main__":
    # Local test
    from sys import stdout

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler(stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # with open("./db_20200626-1203.json") as f:
    #     data = load(f)
    #
    # main_worker(data[:100])

