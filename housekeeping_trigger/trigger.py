#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NoReturn
import logging
from os import getenv
from json import dumps

# 3rd party:
from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")


async def main(timer: TimerRequest, starter: str) -> NoReturn:
    logging.info(f"--- Time trigger has fired: {timer}")

    client = DurableOrchestrationClient(starter)

    instance_id = await client.start_new(
        "housekeeping_orchestrator",
        client_input=dumps({
            "environment": ENVIRONMENT
        })
    )

    logging.info(
        f"Started orchestration for 'housekeeping_orchestrator' with ID = '{instance_id}'."
    )

    return None
