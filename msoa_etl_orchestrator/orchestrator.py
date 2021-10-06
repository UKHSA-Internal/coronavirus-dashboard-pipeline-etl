#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import Tuple
from json import loads, dumps
from os import getenv
from datetime import datetime
from pathlib import Path

# # 3rd party:
from sqlalchemy import select, func, join, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import ProgrammingError

from azure.durable_functions import (
    DurableOrchestrationContext, Orchestrator, RetryOptions
)

from pandas import read_sql, read_csv

# Internal:
try:
    from __app__.db_tables.covid19 import (
        Session, ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from __app__.data_registration import set_file_releaseid
except ImportError:
    from db_tables.covid19 import (
        Session, ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from data_registration import set_file_releaseid

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


DB_URL = getenv("DB_URL")
RECORD_KEY = getenv("RECORD_KEY").encode()

population_path = (
    Path(__file__)
    .resolve()
    .parent
    .parent
    .joinpath("statics", "supplements", "msoa_pop2020.csv")
)


def register_release(category: str, release_id: int):
    insert_stmt = (
        insert(ReleaseCategory.__table__)
        .values(
            release_id=release_id,
            process_name=category
        )
        .on_conflict_do_nothing(
            index_elements=[
                ReleaseCategory.release_id,
                ReleaseCategory.process_name,
            ]
        )
    )

    session = Session()
    try:
        session.begin()
        session.add(insert_stmt)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def get_release_id(datestamp: datetime, process_name: str) -> Tuple[int, datetime]:
    query = (
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
                func.DATE(ReleaseReference.timestamp) == func.DATE(datestamp.isoformat()),
                ReleaseCategory.process_name == process_name
            )
        )
    )

    session = Session()

    try:
        response = session.execute(query)
        result = response.fetchone()

        if result is not None:
            return result

    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    try:
        release = ReleaseReference(timestamp=datestamp)
        session.begin()
        session.add(release)
        session.commit()

        category = ReleaseCategory(release_id=release.id, process_name=process_name)
        session.begin()
        session.add(category)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return get_release_id(datestamp, process_name)


def get_metric_id(metric: str):
    stmt = (
        select([
            MetricReference.id
        ])
        .where(
            MetricReference.metric == metric
        )
    )

    session = Session()
    try:
        result = session.execute(stmt)
        return result.fetchone()[0]

    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()


def get_area_codes(area_type: str):
    query = (
        select([
            AreaReference.id.label("area_id"),
            AreaReference.area_code
        ])
        .where(
            AreaReference.area_type == area_type
        )
    )

    session = Session()
    try:
        result = read_sql(query, con=session.connection())
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return result


def get_msoa_poplation():
    result = (
        read_csv(population_path)
        .set_index(["areaCode"])
        .to_dict()
    )

    return result["population"]


def get_partition_id(area_type, release):
    if area_type in ["nhsTrust", "utla", "ltla", "msoa"]:
        partition_id = f"{release:%Y_%-m_%-d}|{area_type.lower()}"
    else:
        partition_id = f"{release:%Y_%-m_%-d}|other"

    return partition_id


def create_partition(area_type, release):
    area_type = area_type.lower()
    partition_id = get_partition_id(area_type, release)

    if area_type in ["nhstrust", "utla", "ltla", "msoa"]:
        area_partition = f"{release:%Y_%-m_%-d}_{area_type}"
    else:
        area_partition = f"{release:%Y_%-m_%-d}_other"

    # session = Session(autocommit=True)
    session = Session()
    try:
        # session.begin()
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS covid19.time_series_p{area_partition} 
            PARTITION OF covid19.time_series ( partition_id )
            FOR VALUES IN ('{partition_id}');
            """
        )
        session.flush()
    except ProgrammingError as e:
        session.rollback()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return partition_id


# def create_file_record(file_path: str, release_id: int, area_id: int):
#     engine = create_engine(DB_URL, encoding="UTF-8")
#
#     insert_stmt = (
#         insert(ProcessedFile.__table__)
#         .values(
#             file_path=file_path,
#             release_id=release_id,
#             area_id=area_id
#         )
#     )
#
#     update_stmt = (
#         insert_stmt
#         .on_conflict_do_nothing(
#             index_elements=[
#                 ProcessedFile.file_path,
#                 ProcessedFile.release_id,
#                 ProcessedFile.area_id
#             ]
#         )
#         .returning(ProcessedFile.id)
#         # .compile(dialect=postgres())
#     )
#
#     result = engine.execute(update_stmt).fetchone()
#
#     return result[0]


@Orchestrator.create
def main(context: DurableOrchestrationContext):
    logging.info(f"MSOA ETL orchestrator has been triggered")

    trigger_payload: str = context.get_input()
    logging.info(f"\tTrigger received: {trigger_payload}")

    retry_options = RetryOptions(
        first_retry_interval_in_milliseconds=3_000,
        max_number_of_attempts=3
    )

    trigger_data = loads(trigger_payload)

    data_container = trigger_data.get("container", "rawdbdata")
    data_path = trigger_data["data_path"]
    process_name = trigger_data["process_name"]

    area_type = trigger_data["area_type"]
    now = trigger_data.get("timestamp", datetime.now())

    if not isinstance(now, datetime):
        now = datetime.fromisoformat(now)

    metric_name = "newCasesBySpecimenDate"
    area_codes = get_area_codes(area_type.lower())
    release_id, timestamp = get_release_id(now, process_name)
    set_file_releaseid(filepath=trigger_data["main_data_path"], release_id=release_id)

    metric_id = get_metric_id(metric_name)
    population = get_msoa_poplation()
    partition_id = create_partition(area_type, timestamp)

    payload = {
        "data_path": {
            "container": data_container,
            "path": data_path
        },
        "area_type": area_type.lower(),
        "metric": metric_name,
        "partition_id": partition_id,
        "metric_id": metric_id,
        "release_id": release_id,
        "timestamp": timestamp.isoformat()
    }

    logging.info(f"Base payload: {payload}")

    tasks = list()

    context.set_custom_status("Submitting for processing and deployment to DB")
    for _, row in area_codes.iterrows():
        payload.update({
            "area_code": row.area_code,
            "population": population[row.area_code],
            "area_id": row.area_id
        })

        task = context.call_activity_with_retry(
            "msoa_etl_db",
            retry_options=retry_options,
            input_=payload
        )
        tasks.append(task)

    _ = yield context.task_all(tasks)
    context.set_custom_status("DB deployment complete")

    _ = yield context.call_activity_with_retry(
        'db_etl_update_db',
        input_=dict(
            date=f"{now:%Y-%m-%d}",
            category=area_type.lower()
        ),
        retry_options=retry_options
    )
    context.set_custom_status("Metadata updated")

    return f"DONE: {data_path}"


def run_test():
    print(get_release_id(datetime.now(), "msoa"))
    # print(confirm_or_create_area('utla', 'E06000041', 'Wokingham'))


if __name__ == '__main__':
    run_test()
