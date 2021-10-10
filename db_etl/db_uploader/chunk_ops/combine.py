#!/usr/bin python3

# ToDo:
#   Candidate for removal. Investigate dependencies.

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from asyncio import get_event_loop, wait, sleep
from io import BytesIO
from json import loads

# 3rd party:
from pandas import read_feather, DataFrame

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.database import CosmosDB, Collection
except ImportError:
    from storage import StorageClient
    from database import CosmosDB, Collection

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    # 'combine_and_upload_from_file'
]


MAIN_DATA = "daily_chunks/main/{date}/{area_type}_{area_code}.ft"
SUPPLEMENTS = [
    "daily_chunks/publish_date_cases/by_age/{date}/{area_type}_{area_code}.ft",
    "daily_chunks/specimen_date_cases/by_age/{date}/{area_type}_{area_code}.ft",
    "daily_chunks/deaths_28days_death_date/by_age/{date}/{area_type}_{area_code}.ft"
]

INDEX_METRICS = ["areaType", "areaCode", "date"]

UPLOAD_RETRIES = 30

UPLOAD_ERROR_MSG = (
    "Failed to upload row with hash {hash} - "
    "areaType: {areaType} | areaCode: {areaCode} | "
    "areaName: {areaName} | date: {date}"
)


# db_container = CosmosDB(Collection.DATA, writer=True)


async def download_file(container: str, path: str) -> DataFrame:
    logging.info(f"> Downloading data from '{container}/{path}'")

    with StorageClient(container=container, path=path) as client:
        if not client.exists():
            return DataFrame([], columns=["areaType", "areaCode", "date"])

        data = client.download()

    logging.info(f"> Download complete")

    data_io = BytesIO(data.readall())
    data_io.seek(0)
    return read_feather(data_io)


async def get_data(container: str, file_path: str):
    logging.info(f"> Parsing feather data: {file_path}")
    data = await download_file(container, file_path)
    logging.info(f"> Total of {data.shape} was extracted from '{file_path}'")

    data.set_index(INDEX_METRICS, inplace=True, drop=True)
    data.drop(columns=['index'], inplace=True, errors='ignore')

    return data


async def iter_data(data: DataFrame):
    # Easiest way to get this in the desired format
    # is the dump the data to JSON as records and then
    # parse the resulting JSON.
    json_records = data.to_json(orient="records")

    for row in loads(json_records):
        yield row


# async def upsert(row, nested_column_names):
#     for col in nested_column_names:
#         row[col] = row[col] or []
#
#     row_data = dict(filter(lambda x: x[1] is not None, row.items()))
#
#     attempt = 0
#
#     while attempt < UPLOAD_RETRIES:
#         try:
#             db_container.upsert(row_data)
#             return None
#         except Exception as e:
#             logging.warning(e)
#             attempt += 1
#             await sleep(attempt * 2)
#
#     logging.critical(UPLOAD_ERROR_MSG.format(**row_data))


# async def combine_and_upload_from_file(container: str, date: str, area_type: str, area_code: str):
#     logging.info(f"--- Starting the process to combined and upload data from ")
#
#     loop = get_event_loop()
#
#     main_path = MAIN_DATA.format(
#         date=date,
#         area_type=area_type,
#         area_code=area_code
#     )
#     data = await get_data(container, main_path)
#     # data.drop(columns=['index'], inplace=True)
#
#     tasks = list()
#     for template_path in SUPPLEMENTS:
#         filepath = template_path.format(
#             date=date,
#             area_type=area_type,
#             area_code=area_code
#         )
#         tmp = get_data(container, filepath)
#         task = loop.create_task(tmp)
#         tasks.append(task)
#
#     column_names = set()
#     done, pending = await wait(tasks)
#     for future in done:
#         supplement = future.result()
#         column_names = column_names.union(supplement.columns)
#         data = data.join(supplement, on=INDEX_METRICS, how='left')
#
#     column_names = column_names.difference(INDEX_METRICS)
#
#     data.reset_index(inplace=True)
#     data = data.where(data.notnull(), None)
#
#     async for row in iter_data(data):
#         loop.create_task(upsert(row, column_names))


# if __name__ == '__main__':
#     eloop = get_event_loop()
#     eloop.run_until_complete(
#         combine_and_upload_from_file(
#             container="pipeline",
#             date="2020-12-23",
#             area_type="nation",
#             area_code="E92000001"
#         )
#     )
