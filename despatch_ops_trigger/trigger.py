#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NoReturn
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
    logging.info(f"--- SeviceBus event has triggered the function. Starting the process")

    client = DurableOrchestrationClient(starter)
    message = message.get_body().decode()
    logging.info(f"Message: {message}")

    instance_id = await client.start_new(
        "despatch_ops_orchestrator",
        client_input=message
    )

    logging.info(
        f"Started orchestration for 'despatch_ops_orchestrator' with ID = '{instance_id}'."
    )

    return None
