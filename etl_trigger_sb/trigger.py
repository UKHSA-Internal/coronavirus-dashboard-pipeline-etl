#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from json import loads
from typing import NoReturn

# 3rd party:
from azure.functions import ServiceBusMessage
from azure.durable_functions import DurableOrchestrationClient

# Internal:


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")


async def main(message: ServiceBusMessage, starter: str) -> NoReturn:
    logging.info(f"--- Service Bus has triggered the function. Starting the process.")

    raw_message = message.get_body().decode()
    logging.info(f"Message: {raw_message}")

    client = DurableOrchestrationClient(starter)

    instance_id = await client.start_new(
        message.label,
        client_input=raw_message,
        instance_id=message.message_id
    )

    logging.info(f"Started orchestration for '{message.label}' with ID = '{instance_id}'.")

    return None
