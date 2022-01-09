#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NoReturn
from json import loads
import logging

# 3rd party:
from azure.durable_functions import DurableOrchestrationClient
from azure.functions import ServiceBusMessage

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


async def main(message: ServiceBusMessage, starter: str) -> NoReturn:
    logging.info(f"--- ServiceBus event has triggered the function. Starting the process")

    client = DurableOrchestrationClient(starter)
    raw_message = message.get_body().decode()
    message = loads(raw_message)
    logging.info(f"Message: {raw_message}")

    instance_id = await client.start_new(
        "despatch_ops_orchestrator",
        instance_id=message.get("instance_id", None),
        client_input=raw_message
    )

    logging.info(
        f"Started orchestration for 'despatch_ops_orchestrator' with ID = '{instance_id}'."
    )

    return None
