#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from json import loads
from itertools import product

# 3rd party:
from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)
from orjson import loads

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.despatch_ops_cache_buster import get_operations
    from __app__.despatch_ops_release import ReleaseTimestamps
    from __app__.despatch_ops_workers.map_geojson import Device
    from __app__.db_tables.covid19 import (
        ReleaseReference, AreaReference, MetricReference,
        ProcessedFile
    )
except ImportError:
    from storage import StorageClient
    from despatch_ops_cache_buster import get_operations
    from despatch_ops_release import ReleaseTimestamps
    from despatch_ops_workers.map_geojson import Device
    from db_tables.covid19 import (
        ReleaseReference, AreaReference, MetricReference,
        ProcessedFile
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"Despatch ops orchestrator has been triggered")

    trigger_payload: str = context.get_input()
    logging.info(f"\tTrigger received: {trigger_payload}")

    retry_options = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=5
    )

    trigger_data = loads(trigger_payload)

    devices = [Device.desktop, Device.mobile]
    area_types = ["utla", "ltla", "msoa"]

    tasks = list()
    for area_type, device in product(area_types, devices):
        task = context.call_activity_with_retry(
            "despatch_ops_workers",
            retry_options=retry_options,
            input_={
                "handler": "map_geojson",
                "payload": {
                    "area_type": area_type,
                    "device": device,
                    "timestamp": trigger_data["timestamp"]
                }
            }
        )

        tasks.append(task)

    task = context.call_activity_with_retry(
        "despatch_ops_workers",
        retry_options=retry_options,
        input_={
            "handler": "vax_map_geojson",
            "payload": {"timestamp": trigger_data["timestamp"]}
        }
    )

    tasks.append(task)

    area_types = ["nation", "region", "utla", "ltla", "msoa"]
    for area_type in area_types:
        task = context.call_activity_with_retry(
            "despatch_ops_workers",
            retry_options=retry_options,
            input_={
                "handler": "map_percentiles",
                "payload": {
                    "area_type": area_type,
                    "timestamp": trigger_data["timestamp"]
                }
            }
        )

        tasks.append(task)

    task = context.call_activity_with_retry(
        "despatch_ops_workers",
        retry_options=retry_options,
        input_={
            "handler": "archive_dates",
            "payload": {
                "data_type": "MAIN",
                "timestamp": trigger_data["timestamp"]
            }}
    )
    tasks.append(task)

    task = context.call_activity_with_retry(
        "despatch_ops_workers",
        retry_options=retry_options,
        input_={
            "handler": "og_images",
            "payload": {"timestamp": trigger_data["timestamp"]}
        }
    )
    tasks.append(task)

    task = context.call_activity_with_retry(
        "despatch_ops_workers",
        retry_options=retry_options,
        input_={
            "handler": "sitemap",
            "payload": {"timestamp": trigger_data["timestamp"]}
        }
    )
    tasks.append(task)

    task = context.call_activity_with_retry(
        "despatch_ops_workers",
        retry_options=retry_options,
        input_={
            "handler": "landing_page_map",
            "payload": {"timestamp": trigger_data["timestamp"]}
        }
    )
    tasks.append(task)

    context.set_custom_status("All jobs created - submitting for execution.")
    _ = yield context.task_all(tasks)

    context.set_custom_status("All jobs complete - updating timestamps.")

    tasks = list()
    for item in ReleaseTimestamps:
        processor_fn = item["process"]
        value = processor_fn(trigger_data["releaseTimestamp"])

        task = context.call_activity_with_retry(
            "despatch_ops_cache_buster",
            retry_options=retry_options,
            input_={
                "path": item["path"],
                "container": item["container"],
                "value": value
            }
        )

        tasks.append(task)

    _ = yield context.task_all(tasks)
    context.set_custom_status("Timestamps updated - clearing Redis cache.")

    tasks = list()
    for payload in get_operations():
        task = context.call_activity_with_retry(
            "despatch_ops_cache_buster",
            retry_options=retry_options,
            input_=payload
        )

        tasks.append(task)

    _ = yield context.task_all(tasks)

    context.set_custom_status(f"ALL DONE: {trigger_payload}")

    return f"ALL DONE: {trigger_payload}"
