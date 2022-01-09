#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NoReturn
import logging

# 3rd party:
from azure.functions import ServiceBusMessage
from azure.durable_functions import DurableOrchestrationClient

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    'main'
]


async def main(message: ServiceBusMessage, starter: str) -> NoReturn:
    logging.info(f"--- ServiceBus event has triggered the function. Starting the process.")

    raw_message = message.get_body().decode()
    logging.info(f"Message: {raw_message}")

    client = DurableOrchestrationClient(starter)
    instance_id = await client.start_new(
        "cache_buster_orchestrator",
        client_input=raw_message,
        instance_id=message.message_id
    )

    logging.info(
        f"Started orchestration for 'cache_buster_orchestrator' with ID = '{instance_id}'."
    )

    return None
