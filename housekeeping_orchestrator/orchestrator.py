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
    from __app__.data_registration import register_file
    from .dtypes import RetrieverPayload, GenericPayload
except ImportError:
    from data_registration import register_file
    from housekeeping_orchestrator.dtypes import RetrieverPayload, GenericPayload

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    retry_twice_opts = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=2
    )

    timestamp = context.current_utc_datetime
    trigger_payload = loads(context.get_input())

    logging.info(f"Triggered with payload: {trigger_payload}")

    # Retrieve blob paths
    context.set_custom_status("Retrieving candidates")
    candidates = yield context.call_activity_with_retry(
        "housekeeping_retriever",
        input_=RetrieverPayload(
            timestamp=timestamp.isoformat(),
            environment=trigger_payload['environment'],
        ),
        retry_options=retry_twice_opts
    )

    # Submit for archiving
    context.set_custom_status("Submitting candidates to the archiver")
    activities = list()
    for artefact in candidates:
        activity = context.call_activity_with_retry(
            "housekeeping_archiver",
            input_=GenericPayload(
                timestamp=timestamp.isoformat(),
                environment=trigger_payload['environment'],
                content=artefact,
            ),
            retry_options=retry_twice_opts
        )
        activities.append(activity)

    archived = yield context.task_all(activities)

    # Dispose of archived blobs
    context.set_custom_status("Removing archived data")
    activities = list()
    for archived_file in archived:
        activity = context.call_activity_with_retry(
            "housekeeping_disposer",
            input_=GenericPayload(
                timestamp=timestamp.isoformat(),
                environment=trigger_payload['environment'],
                content=archived_file,
            ),
            retry_options=retry_twice_opts
        )
        activities.append(activity)

    report = yield context.task_all(activities)

    context.set_custom_status(f"ALL DONE - processed {report['total_processed']} artefacts")

    return f"DONE - {timestamp.isoformat()}"
