#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from json import loads
from itertools import chain

# 3rd party:
from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

# Internal:
try:
    from .dtypes import RetrieverPayload, GenericPayload, Manifest, ProcessMode
    from .tasks import housekeeping_tasks
except ImportError:
    from housekeeping_orchestrator.dtypes import (
        RetrieverPayload, GenericPayload, Manifest, ProcessMode
    )
    from housekeeping_orchestrator.tasks import housekeeping_tasks

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

    logging.info(f"triggered with payload: {trigger_payload}")

    # ------------------------------------------------------------------------------------
    # Retrieve blob paths
    # ------------------------------------------------------------------------------------
    context.set_custom_status("Retrieving artefacts")
    logging.info("retrieving artefacts")

    task_artefacts = list()

    for task_manifest in housekeeping_tasks:
        logging.info(f"submitting '{task_manifest['label']}' to retriever")

        artefacts = context.call_activity_with_retry(
            "housekeeping_retriever",
            input_=RetrieverPayload(
                timestamp=timestamp.isoformat(),
                environment=trigger_payload['environment'],
                manifest=task_manifest
            ),
            retry_options=retry_twice_opts
        )

        task_artefacts.append(artefacts)

    logging.info("awaiting retriever tasks")
    retrieved_artefacts = yield context.task_all(task_artefacts)

    # ------------------------------------------------------------------------------------
    # Submit for archiving
    # ------------------------------------------------------------------------------------
    context.set_custom_status("Submitting candidates to the archiver")
    logging.info("submitting candidates to the archiver")

    archive_modes = [ProcessMode.ARCHIVE_AND_DISPOSE, ProcessMode.ARCHIVE_ONLY]
    activities = list()

    for task in chain(*retrieved_artefacts):
        logging.info(f"submitting '{task['manifest']['label']}' to retriever")

        if task["manifest"]["mode"] not in archive_modes:
            logging.info("-- not archived")
            continue

        activity = context.call_activity_with_retry(
            "housekeeping_archiver",
            input_=task,
            retry_options=retry_twice_opts
        )
        activities.append(activity)

    logging.info("awaiting archiver tasks")
    archived_artefacts = yield context.task_all(activities)

    # ------------------------------------------------------------------------------------
    # Dispose of archived blobs
    # ------------------------------------------------------------------------------------
    context.set_custom_status("Removing archived data")
    logging.info("removing archived data")

    disposable_only = filter(
        lambda t: t['manifest']['mode'] == ProcessMode.DISPOSE_ONLY,
        task_artefacts
    )

    disposal_modes = [ProcessMode.ARCHIVE_AND_DISPOSE, ProcessMode.DISPOSE_ONLY]
    activities = list()

    for task in chain(archived_artefacts, disposable_only):
        logging.info(f"submitting '{task['manifest']['label']}' to retriever")

        if task["manifest"]["mode"] not in disposal_modes:
            logging.info("-- not disposed")
            continue

        activity = context.call_activity_with_retry(
            "housekeeping_disposer",
            input_=task,
            retry_options=retry_twice_opts
        )
        activities.append(activity)

    logging.info("awaiting disposer tasks")
    report = yield context.task_all(activities)

    # ------------------------------------------------------------------------------------

    context.set_custom_status(f"ALL DONE - processed {report['total_processed']} artefacts")

    return f"DONE - {timestamp.isoformat()}"
