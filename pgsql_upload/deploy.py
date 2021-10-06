#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from asyncio import get_event_loop
from base64 import decodebytes
from json import loads

# 3rd party:

from azure.functions import QueueMessage


# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.fanout import enqueue_job
    from __app__.db_etl_upload import main as process_data
except ImportError:
    from storage import StorageClient
    from fanout import enqueue_job
    from db_etl_upload import main as process_data

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'run_jobs'
]


async def processor(**context):
    process_data(**context)


async def run_jobs(message: QueueMessage):
    """
    Triggered by the queue (entrypoint) to run the job.

    Parameters
    ----------
    message: QueueMessage
    """
    message_json_bytes = decodebytes(message.get_body())
    message_json = message_json_bytes.decode()

    data = loads(message_json)

    context = data['context']

    task = processor(**context)

    event_loop = get_event_loop()
    event_loop.create_task(task)
