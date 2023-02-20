#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from datetime import datetime
from json import loads
from typing import Union, Tuple
from random import random
from time import sleep

# 3rd party:
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, create_engine, func, join, and_

from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.utilities.data_files import category_label, parse_filepath
    from __app__.db_tables.covid19 import (
        ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from __app__.data_registration import set_file_releaseid
except ImportError:
    from storage import StorageClient
    from utilities.data_files import category_label, parse_filepath
    from db_tables.covid19 import (
        ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from data_registration import set_file_releaseid

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
    'category_label'
]

DB_URL = getenv("DB_URL")
# ENVIRONMENT = getenv("API_ENV")

WEBSITE_TIMESTAMP_KWS = dict(
    container="publicdata",
    path="assets/dispatch/website_timestamp",
    content_type="text/plain; charset=utf-8",
    cache_control="no-cache, max-age=0",
    compressed=False
)

engine = create_engine(DB_URL, connect_args={'charset':'utf8'}, pool_size=30, max_overflow=-1)
Session = sessionmaker(bind=engine)


def get_release_id(timestamp: Union[str, datetime], process_name: str) -> Tuple[int, datetime]:
    # Prevent duplication
    sleep(random() * 10)

    session = Session()

    try:
        query = session.execute(
            select([
                ReleaseReference.id,
                ReleaseReference.timestamp
            ])
                .select_from(
                join(
                    ReleaseReference, ReleaseCategory,
                    ReleaseReference.id == ReleaseCategory.release_id
                )
            )
                .where(
                and_(
                    func.DATE(ReleaseReference.timestamp) == timestamp.date(),
                    ReleaseCategory.process_name == process_name
                )
            )
        )

        result = query.fetchone()

        if result is not None:
            return result

    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    session = Session(autocommit=True)
    try:
        release = ReleaseReference(timestamp=timestamp)
        session.add(release)
        session.flush()

        category = ReleaseCategory(release_id=release.id, process_name=process_name)
        session.add(category)
        session.flush()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return get_release_id(timestamp, process_name)


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"DB ETL orchestrator has been triggered")

    retry_options = RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=5
    )

    trigger_payload: str = context.get_input()
    logging.info(f"\tTrigger received: {trigger_payload}")

    trigger_data = loads(trigger_payload)

    timestamp = trigger_data["datestamp"]
    datestamp = trigger_data["datestamp"].split("T")[0]

    if "path" in trigger_data:
        paths = [
            trigger_data["path"]
        ]
    else:
        paths = [
            # f"daily_chunks/specimen_date_cases/by_age/{datestamp}/",
            # f"daily_chunks/deaths_28days_death_date/by_age/{datestamp}/",
            f"daily_chunks/main/{datestamp}/"
        ]

    category = trigger_data.get("category", "main")

    main_path = trigger_data['main_data_path']
    if main_path.endswith("json"):
        process_name = "MAIN"
    else:
        parsed_path = parse_filepath(main_path)
        process_name = category_label(parsed_path)

    tasks = list()

    if len(timestamp) > 10:
        now = datetime.fromisoformat(timestamp)
    else:
        file_date = datetime.strptime(datestamp, "%Y-%m-%d")
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

    release_id, timestamp = get_release_id(now, process_name)
    set_file_releaseid(filepath=trigger_data["main_data_path"], release_id=release_id)

    payload = {
        "timestamp": timestamp.isoformat()
    }

    for path in paths:
        with StorageClient(container="pipeline", path=path) as client:
            for file in client:
                payload.update({
                    'file_path': file['name']
                })

                task = context.call_activity_with_retry(
                    "db_etl_upload",
                    retry_options=retry_options,
                    input_=payload
                )
                tasks.append(task)

    _ = yield context.task_all(tasks)
    context.set_custom_status("Upload to database is complete.")

    if category != "main":
        # Categories other than main may have DB level processes. These
        # need to be performed before stats and graphs are generated.
        # Processes for stats and graphs are therefore moved to chunk
        # processor.
        context.set_custom_status(
            "Chunk deployment is done. Remaining processes are skipped."
        )
        return f"DONE: {trigger_payload}"

    settings_task = context.call_activity_with_retry(
        'db_etl_update_db',
        input_=dict(
            date=f"{now:%Y-%m-%d}",
            process_name=process_name,
            environment=trigger_data.get('environment', "PRODUCTION")
        ),
        retry_options=retry_options
    )

    graphs_task = context.call_activity_with_retry(
        'db_etl_homepage_graphs',
        input_=dict(
            date=f"{now:%Y-%m-%d}",
            category=category
        ),
        retry_options=retry_options
    )

    _ = yield context.task_all([settings_task, graphs_task])

    context.set_custom_status("Metadata updated / graphs created.")

    return f"DONE: {trigger_payload}"
