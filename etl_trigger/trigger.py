#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from json import loads, dumps

# 3rd party:
from azure.functions import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient

# Internal:


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")


async def main(req: HttpRequest, starter: str) -> HttpResponse:
    logging.info(f"--- Web hook has triggered the function. Starting the process")

    client = DurableOrchestrationClient(starter)

    payload: str = req.get_body().decode()
    parsed_payload = loads(payload)
    parsed_payload["ENVIRONMENT"] = ENVIRONMENT
    payload = dumps(parsed_payload, separators=(',', ':'))

    func_name = req.route_params["functionName"]
    instance_id = req.params.get("instance_id", None)

    instance_id = await client.start_new(
        func_name,
        client_input=payload,
        instance_id=instance_id
    )

    logging.info(f"Started orchestration for '{func_name}' with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)
