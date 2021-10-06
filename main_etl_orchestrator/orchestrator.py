#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from json import loads, dumps
from datetime import datetime
from os import getenv
from random import random
from time import sleep

# 3rd party:
from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

# Internal:
try:
    from __app__.data_registration import register_file
except ImportError:
    from data_registration import register_file

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"Main ETL orchestrator has been triggered")

    default_retry_opts = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=6
    )

    retry_twice_opts = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=2
    )

    trigger_payload: str = context.get_input()
    logging.info(f"\tTrigger received: {trigger_payload}")

    trigger_data = loads(trigger_payload)
    environment = trigger_data.get("ENVIRONMENT", ENVIRONMENT)

    file_name: str = trigger_data['fileName']

    raw_timestamp = trigger_data.get('timestamp', context.current_utc_datetime.isoformat())[:26]
    logging.info(f"Process timestamp: {raw_timestamp}")

    file_date_raw, _ = raw_timestamp.split("T")
    file_date = datetime.strptime(file_date_raw, "%Y-%m-%d")
    now = context.current_utc_datetime
    now = datetime(
        year=file_date.year,
        month=file_date.month,
        day=file_date.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
        microsecond=now.microsecond
    )

    if not context.is_replaying:
        # Sleep for a random period to allow de-sync
        # concurrent triggers.
        sleep_time = random() * 10
        sleep(sleep_time)

        logging.info(f"Not replaying - registering '{file_name}'")
        register_file(filepath=file_name, timestamp=now, instance_id=context.instance_id)

    if not file_name.endswith("json"):
        context.set_custom_status(f"Identified as non-JSON: {file_name}.")
        _ = yield context.call_sub_orchestrator_with_retry(
            "chunk_etl_orchestrator",
            input_=dumps({
                "fileName": file_name,
                "environment": environment,
            }),
            retry_options=retry_twice_opts
        )

        return f"DONE: {trigger_data}"

    logging.info("Following the main data pathway.")

    # Determine whether or not the payload is for
    # processing legacy data.
    # NOTE: Legacy data do not get:
    #       - deployed to the database,
    #       - archived,
    #       - a new despatch token.
    is_legacy = trigger_data.get("legacy", False)
    logging.info(f"> Legacy mode: {is_legacy}")

    # Generate retrieve payload
    retrieve_payload = {
        'data_path': file_name,
        'timestamp':  f"{raw_timestamp:0<26}",
        'legacy': is_legacy
    }
    logging.info(
        f'\tTrigger payload parsed - '
        f'processing "{retrieve_payload["data_path"]}" @ '
        f'"{retrieve_payload["timestamp"]}"'
    )

    # Read file and split into chunks by area type
    logging.info(f'\tStarting the process to retrieve new data')
    area_data_paths = yield context.call_activity_with_retry(
        "main_etl_retrieve_data",
        input_=retrieve_payload,
        retry_options=retry_twice_opts
    )
    logging.info(f'\tDOWNLOAD COMPLETE')
    context.set_custom_status("Data file has been parsed.")

    # Process chunks
    logging.info(f'Starting the main process')

    tasks = list()
    for data_path in area_data_paths:
        task = context.call_activity_with_retry(
            "main_etl_processor",
            input_=dict(
                data_path=data_path,
                timestamp=retrieve_payload['timestamp'] + "5Z",
                environment=environment
            ),
            retry_options=default_retry_opts
        )
        tasks.append(task)

    # Await processes
    etl_response = yield context.task_all(tasks)
    logging.info(f'>>> ALL MAIN ETL PROCESSES COMPLETE - length: {len(etl_response)}')
    context.set_custom_status("Main ETL processes are done. Creating box plot.")

    if is_legacy is True:
        context.set_custom_status("Legacy file detected.")
        return f"DONE: {context.current_utc_datetime}"

    # ToDo: To be removed:
    # [START]
    # Generating despatch token [DEPRECATED]
    # token_response = context.call_activity(
    #     "main_etl_token_generator",
    #     input_=dict(
    #         environment=retrieve_payload['ENVIRONMENT'],
    #         path=retrieve_payload['data_path']
    #     )
    # )
    # tasks.append(token_response)
    #
    # Deploy to the database
    # for payload in etl_response:
    #     task = context.call_activity_with_retry(
    #         name="main_etl_deploy_to_db",
    #         retry_options=default_retry_opts,
    #         input_=payload,
    #     )
    #     tasks.append(task)
    #
    # _ = yield context.task_all(tasks)
    # [END]

    _ = yield context.call_activity_with_retry(
        "chunk_etl_postprocessing",
        input_={
            "timestamp": now.isoformat(),
            "environment": environment,
            "category": "main"
        },
        retry_options=retry_twice_opts
    )

    context.set_custom_status("Deploying to the DB.")

    _ = yield context.call_sub_orchestrator_with_retry(
        "db_etl_orchestrator",
        input_=dumps({
            "datestamp": now.isoformat(),
            "environment": ENVIRONMENT,
            "main_data_path": file_name
        }),
        retry_options=retry_twice_opts
    )

    context.set_custom_status("Submitting main postprocessing tasks")
    _ = yield context.call_activity_with_retry(
        "main_etl_postprocessors",
        input_=dict(
            original_path=retrieve_payload['data_path'],
            timestamp=raw_timestamp,
            environment=environment
        ),
        retry_options=retry_twice_opts
    )
    logging.info("Done with latest main_etl_postprocessors.")

    # ====================================================================================

    tasks = list()

    # Retrieve scales
    context.set_custom_status("Requesting latest scale records.")

    area_types = ["nation", "region", "utla", "ltla", "msoa"]

    for area_type in area_types:
        task = context.call_activity_with_retry(
            "rate_scales_worker",
            retry_options=retry_twice_opts,
            input_={
                "type": "RETRIEVE",
                "timestamp": raw_timestamp,
                "area_type": area_type
            }
        )
        tasks.append(task)

    raw_scale_records = yield context.task_all(tasks)
    logging.info("Received latest scale records.")

    # ------------------------------------------------------------------------------------

    context.set_custom_status("Creating post deployment tasks")

    # Concatenate and archive processed data
    archive_response = context.call_activity_with_retry(
        "main_etl_archiver",
        input_=dict(
            results=etl_response,
            original_path=retrieve_payload['data_path'],
            timestamp=retrieve_payload['timestamp'] + "5Z",
            environment=environment
        ),
        retry_options=retry_twice_opts
    )
    logging.info("Created jobs for `main_etl_archiver`")

    # ....................................................................................
    # Pre-populate cache

    populate_cache = context.call_activity_with_retry(
        "cache_prepopulate",
        input_=dict(
            timestamp=raw_timestamp,
            environment=environment
        ),
        retry_options=retry_twice_opts
    )
    logging.info("Created jobs for `cache_prepopulate`")

    # ....................................................................................

    # Send daily report email
    daily_report = context.call_activity(
        "main_etl_daily_report",
        input_=dict(
            legacy=is_legacy,
            timestamp=raw_timestamp,
            environment=environment
        )
    )
    logging.info("Created jobs for `main_etl_daily_report`")

    # ....................................................................................

    tasks = [
        daily_report,
        archive_response,
        populate_cache
    ]

    # ....................................................................................
    # Generate rate scales

    for item in raw_scale_records:
        for record in item['records']:
            task = context.call_activity_with_retry(
                "rate_scales_worker",
                retry_options=retry_twice_opts,
                input_={
                    "type": "GENERATE",
                    "date": file_date_raw,
                    "timestamp": item["timestamp"],
                    "area_type": record['area_type'],
                    "area_code": record['area_code'],
                    "rate": record['rate'],
                    "percentiles": item['percentiles'],
                }
            )
            tasks.append(task)

    logging.info("Created jobs for `rate_scales_worker`")
    # ....................................................................................

    context.set_custom_status("Submitting post deployment tasks")
    _ = yield context.task_all(tasks)
    context.set_custom_status("ALL done.")

    return f"DONE: {trigger_data}"
