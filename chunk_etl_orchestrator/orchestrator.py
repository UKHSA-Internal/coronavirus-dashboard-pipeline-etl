#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import re
import logging
from json import loads, dumps
from datetime import datetime

# 3rd party:
from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

# Internal:
try:
    from __app__.utilities.data_files import parse_filepath, category_label
    from __app__.data_registration import register_file
except ImportError:
    from utilities.data_files import parse_filepath, category_label
    from data_registration import register_file

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"Chunk ETL orchestrator has been triggered")

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

    file_name = trigger_data["fileName"]

    metadata = parse_filepath(file_name)

    if metadata is None:
        # Path pattern does not conform
        # to the defined pattern.
        context.set_custom_status("File name cannot be parsed. Process terminated.")
        return f"DONE: {trigger_data}"

    now = context.current_utc_datetime
    timestamp_raw = (
        datetime
        .strptime(f'{metadata["timestamp"]}{now:%S}.{now:%f}', "%Y%m%d%H%M%S.%f")
    )
    timestamp = timestamp_raw.isoformat()

    main_path = trigger_data['fileName']
    if main_path.endswith("json"):
        process_name = "MAIN"
    else:
        process_name = category_label(metadata)

    msg = (
        f'Starting to upload pre-processed data: '
        f'{metadata["area_type"]}::{metadata["category"]}::{metadata["subcategory"]}'
    )

    if (metadata["area_type"], metadata["category"]) == ("MSOA", "vaccinations-by-vaccination-date"):
        logging.info(msg)
        context.set_custom_status(msg)

        process_name = "MSOA: VACCINATION - EVENT DATE"

        _ = yield context.call_activity_with_retry(
            "chunk_db_direct",
            input_={
                'data_path': file_name,
                'area_type': metadata["area_type"],
                'timestamp': timestamp,
                'process_name': process_name
            },
            retry_options=retry_twice_opts
        )
        logging.info(f"DONE: {msg}")
        context.set_custom_status(f"DONE: {msg}")

    elif (metadata["area_type"], metadata["category"]) == ("MSOA", "cases-by-specimen-date"):
        logging.info(msg)
        context.set_custom_status(msg)

        process_name = "MSOA"

        _ = yield context.call_sub_orchestrator_with_retry(
            "msoa_etl_orchestrator",
            input_=dumps({
                'data_path': file_name,
                'area_type': metadata["area_type"],
                'timestamp': timestamp,
                'process_name': process_name,
                'main_data_path': file_name
            }),
            retry_options=retry_twice_opts
        )
        logging.info(f"DONE: {msg}")
        context.set_custom_status(f"DONE: {msg}")

    else:
        # Read file and split into chunks
        # by area type / area code combination.
        logging.info(f'\tStarting the process to retrieve new data')
        context.set_custom_status("Parsing the payload")
        area_data_paths = yield context.call_activity_with_retry(
            "chunk_etl_retriever",
            input_={
                'path': file_name,
                'date': metadata["date"],
                'area_type': metadata["area_type"],
                'category': metadata["category"],
                'subcategory': metadata["subcategory"],
                'timestamp': timestamp
            },
            retry_options=retry_twice_opts
        )
        logging.info(f'\tDOWNLOAD COMPLETE')
        context.set_custom_status("Payload has been parsed")

        # Process chunks
        logging.info(f'Starting the main process')

        tasks = list()
        context.set_custom_status("Submitting main ETL processes")

        # Create ETL tasks based on the paths
        # returned by `chunk_etl_retriever`.
        for item in area_data_paths:
            data_path = item.pop("path")
            task = context.call_activity_with_retry(
                "chunk_etl_processor",
                input_=dict(
                    base=dict(
                        data_path=data_path,
                        timestamp=timestamp,
                        environment="PRODUCTION"
                    ),
                    **item
                ),
                retry_options=default_retry_opts
            )
            tasks.append(task)

        context.set_custom_status("Awaiting ETL processes")
        # Await processes
        etl_response = yield context.task_all(tasks)
        logging.info(f'>>> ALL MAIN ETL PROCESSES COMPLETE - length: {len(etl_response)}')

        chunks_path = f"daily_chunks/{metadata['category']}/{metadata['date']}/"
        if metadata['subcategory']:
            chunks_path = f"daily_chunks/{metadata['category']}/{metadata['subcategory']}/{metadata['date']}/"

        # Deploy processed data to the DB.
        context.set_custom_status(f"Deploying to the database: {chunks_path}")

        _ = yield context.call_sub_orchestrator_with_retry(
            "db_etl_orchestrator",
            input_=dumps({
                "datestamp": metadata['date'],
                "path": chunks_path,
                "environment": "PRODUCTION",
                "area_type": metadata['area_type'],
                "category": metadata['category'],
                "subcategory": metadata['subcategory'],
                "main_data_path": file_name
            }),
            retry_options=retry_twice_opts
        )

    context.set_custom_status("Postprocessing")
    _ = yield context.call_activity_with_retry(
        "chunk_etl_postprocessing",
        input_={
            "timestamp": timestamp,
            "environment": "PRODUCTION",
            "category": metadata['category'],
            "subcategory": metadata['subcategory'] if metadata['subcategory'] != "" else None,
            "area_type": metadata['area_type'] if metadata['area_type'] != "" else None
        },
        retry_options=retry_twice_opts
    )
    context.set_custom_status(
        "Deployment to the DB is complete, submitting postprocessing tasks."
    )

    settings_task = context.call_activity_with_retry(
        'db_etl_update_db',
        input_=dict(
            date=f"{timestamp_raw:%Y-%m-%d}",
            process_name=process_name,
            environment=trigger_data['environment']
        ),
        retry_options=retry_twice_opts
    )

    context.set_custom_status("Submitting main postprocessing tasks")
    post_processes = context.call_activity_with_retry(
        "main_etl_postprocessors",
        input_=dict(
            original_path=file_name,
            timestamp=timestamp,
            environment=trigger_data['environment']
        ),
        retry_options=retry_twice_opts
    )

    graphs_task = context.call_activity_with_retry(
        'db_etl_homepage_graphs',
        input_=dict(
            date=f"{timestamp_raw:%Y-%m-%d}",
            category=metadata['category'],
            subcategory=metadata['subcategory']
        ),
        retry_options=retry_twice_opts
    )

    _ = yield context.task_all([graphs_task, settings_task, post_processes])

    context.set_custom_status(
        "Metadata updated / graphs created / main postprocessing tasks complete. ALL DONE."
    )

    return f"DONE: {trigger_data}"
