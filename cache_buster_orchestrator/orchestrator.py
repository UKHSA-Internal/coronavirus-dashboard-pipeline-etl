#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from json import loads

# 3rd party:
from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

# Internal:
try:
    from __app__.cache_buster_activity import get_operations
except ImportError:
    from cache_buster_activity import get_operations

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]

FLUSH_DESPATCH = "DESPATCH"
PURGE_STORAGE_CACHE = "PURGE_STORAGE_CACHE"


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"Cache buster orchestrator has been triggered")

    trigger_payload_raw: str = context.get_input()
    logging.info(f"\tTrigger received: {trigger_payload_raw}")

    trigger_payload = loads(trigger_payload_raw)

    retry_options = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=5
    )

    tasks = list()

    if trigger_payload['to'] == FLUSH_DESPATCH:
        for payload in get_operations():
            task = context.call_activity_with_retry(
                "cache_buster_activity",
                retry_options=retry_options,
                input_=payload
            )

            tasks.append(task)

        context.set_custom_status(f"Submitting {len(tasks)} tasks for {FLUSH_DESPATCH}.")

    elif trigger_payload['to'] == PURGE_STORAGE_CACHE:
        task = context.call_activity_with_retry(
            "cache_buster_storage_activity",
            retry_options=retry_options,
            input_=trigger_payload
        )

        tasks.append(task)

        context.set_custom_status(f"Submitting tasks for {PURGE_STORAGE_CACHE}.")

    _ = yield context.task_all(tasks)

    context.set_custom_status(f"ALL DONE: {trigger_payload}")

    return f"ALL DONE: {trigger_payload}"
