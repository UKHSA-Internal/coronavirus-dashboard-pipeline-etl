#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import NoReturn
from os import getenv
from json import loads, dumps
from base64 import decodebytes, encodebytes
from asyncio import get_event_loop
from importlib import import_module
from gzip import compress

# 3rd party:
from azure.functions import QueueMessage
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy
# from azure.servicebus import ServiceBusClient, QueueClient as SB_QueueClient
# from azure.servicebus import Message
# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'enqueue_job',
    'run_jobs',
    'async_enqueue_sb_job'
]


STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
SB_CONNECTION_STRING = getenv("AzureWebJobsEventsSB")
QUEUE_NAME = "fanout"

queue: QueueClient = QueueClient.from_connection_string(
    STORAGE_CONNECTION_STRING,
    queue_name=QUEUE_NAME,
    message_encode_policy=BinaryBase64EncodePolicy()
)


def prep_message(module: str, handler: str, compressed: bool = False, **context):
    data = {
        "module": module,
        "handler": handler,
        "context": context
    }

    message = dumps(data, separators=(",", ":"))

    message_bytes = message.encode()

    if compressed:
        message_bytes = compress(message_bytes)

    message_b64 = encodebytes(message_bytes)

    return message_b64


async def async_enqueue_sb_job(module: str, handler: str, **context):
    msg = prep_message(module, handler, compressed=True, **context)
    # cli = ServiceBusClient.from_connection_string(SB_CONNECTION_STRING)
    # async with cli:
    #     sender = cli.get_queue(queue_name=QUEUE_NAME)

    # cli = SB_QueueClient.from_connection_string(SB_CONNECTION_STRING, QUEUE_NAME)
    # cli.send(Message(msg))

    return None


def enqueue_job(module: str, handler: str, **context) -> NoReturn:
    """
    Enqueues ``context`` to fan out and run using ``module.handler``.

    Parameters
    ----------
    module: str
    handler: str
    context: Dict[str, Any]
    """
    message_b64 = prep_message(module, handler, compressed=False, **context)

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    logging.info(f"Enqueued job context for '{module}.{handler}'")

    queue.send_message(message_b64)


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

    module_path = data['module']
    handler = data['handler']
    context = data['context']

    try:
        # Azure Functions access
        module = import_module(module_path, "__app__")
    except ModuleNotFoundError:
        # Local access
        module = import_module(module_path)

    logging.info(f"Loading module '{module_path}'")

    func = getattr(module, handler)

    task = func(**context)

    event_loop = get_event_loop()
    event_loop.create_task(task)
