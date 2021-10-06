#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       08 Nov 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from asyncio import get_event_loop
from json import loads
from os import getenv
from typing import NamedTuple

# 3rd party:

# Internal:
from azure.cosmos.cosmos_client import CosmosClient

try:
    from __app__.utilities.settings import PUBLICDATA_PARTITION
    from __app__.utilities import get_release_timestamp
    from __app__.storage import StorageClient
except ImportError:
    from utilities.settings import PUBLICDATA_PARTITION
    from utilities import get_release_timestamp
    from storage import StorageClient

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


LATEST_TIMESTAMP = get_release_timestamp(raw=True)
SPROC_NAME = "updateById"


class Credentials(NamedTuple):
    host = getenv("AzureCosmosHost")
    key = getenv('AzureCosmosWriterKey')
    db_name = getenv("AzureCosmosDBName")
    collection = getenv("AzureCosmosCollection")


coll_link = f'dbs/{Credentials.db_name}/colls/{Credentials.collection}/'

if PUBLICDATA_PARTITION == "releaseDate":
    partition_value = LATEST_TIMESTAMP.split("T")[0]
else:
    partition_value = LATEST_TIMESTAMP


db_options = {
    'setScriptLoggingEnabled': True,
    'partitionKey': partition_value
}

db_client = CosmosClient(
    url=Credentials.host,
    credential={'masterKey': Credentials.key},
)

client_conn = db_client.client_connection

sp_query = f"select TOP 1 r._self from r WHERE r.id = '{SPROC_NAME}'"
sprocs = list(client_conn.QueryStoredProcedures(coll_link, sp_query))
sproc_url = sprocs.pop(0)["_self"]


async def to_db(payload):
    """
    Expected payload format:

        [
            "<ID>",
            {"$set": {"<RecordName>": ...}}
        ]


    Parameters
    ----------
    payload

    Returns
    -------

    """
    client_conn.ExecuteStoredProcedure(
        sproc_url,
        params=payload,
        options=db_options
    )


async def from_jsonl(container: str, filepath: str):
    with StorageClient(container=container, path=filepath) as client:
        jsonl = client.download().readall().decode()

    for row in jsonl.splitlines():
        yield loads(row)


async def upload_from_file(container: str, filepath: str):
    """
    Expected format of the message:

        {
            "path/to/file.jsonl"
        }

    Parameters
    ----------
    container
    filepath
    """
    even_loop = get_event_loop()

    async for payload in from_jsonl(container, filepath):
        task = to_db(payload)
        even_loop.create_task(task)

    return True


# if __name__ == "__main__":
#     from os import scandir
#     from asyncio import gather
#
#     loop = get_event_loop()
#
#     tasks = list()
#     # paths = list()
#     for dir_entry in scandir("/Users/pouria/Documents/Projects/coronavirus-data-etl/BlobTrigger_db_data/sample_demogs"):
#         if not dir_entry.is_file() or not dir_entry.name.endswith("jsonl"):
#             continue
#
#         print(dir_entry.path)
#         # paths.append(dir_entry.path)
#         tsk = loop.create_task(upload_demographic_data(dir_entry.path))
#         tasks.append(tsk)
#
#     loop.run_until_complete(gather(*tasks))
