#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       07 Nov 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from json import dumps, loads
from datetime import datetime
from io import StringIO
from pandas import read_csv
from typing import NamedTuple, List, Union, Any
from asyncio import sleep
import logging

# 3rd party:
from pandas import DataFrame, json_normalize

# Internal

try:
    from __app__.fanout import enqueue_job
    from __app__.utilities.settings import PUBLICDATA_PARTITION
    from __app__.utilities.latest_data import get_latest_csv, get_timestamp_for, get_release_timestamp
    from __app__.storage import StorageClient
    # from __app__.dispatch.database import get_latest_count as db_records_count
    from __app__.db_etl.db_uploader.chunk_ops import upload_chunk_feather
except ImportError:
    from fanout import enqueue_job
    from utilities.settings import PUBLICDATA_PARTITION
    from utilities.latest_data import get_latest_csv, get_timestamp_for, get_release_timestamp
    from storage import StorageClient
    # from dispatch.database import get_latest_count as db_records_count
    from db_etl.db_uploader.chunk_ops import upload_chunk_feather

try:
    from ..categories import DemographicsCategory, DemographicsCategoryItem
except ImportError:
    from demographic_etl.categories import DemographicsCategory, DemographicsCategoryItem

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'process_by_age',
    'DemographicsCategory',
    'get_latest_count'
]


STORAGE_CONTAINER = "pipeline"
ID_LABEL = "hash"
LATEST_TIMESTAMP = get_release_timestamp(raw=True)
JSONL_PATH = "demographic/{directory}/by_age/upload2db/{area_type}_{area_code}.jsonl"
FEATHER_PATH = "daily_chunks/{directory}/by_age/{date}"
FEATHER_NAME = "{area_type}_{area_code}.ft"
COUNT_PATH = "demographic/{directory}/by_age/latest_count.json"

queue_kws = dict(
    module="demographic_etl.db_uploader",
    handler="upload_from_file",
    container=STORAGE_CONTAINER
)


count_kws = dict(
    container=STORAGE_CONTAINER,
    compressed=False,
    content_type='application/json; charset=utf-8'
)


if PUBLICDATA_PARTITION == "releaseDate":
    partition_value = LATEST_TIMESTAMP.split("T")[0]
else:
    partition_value = LATEST_TIMESTAMP

index_metrics = [
    "areaType",
    "areaCode",
    "date"
]

label_replacements = {
    "0_4": "00_04",
    "5_9": "05_09",
    "0_59": "00_59"
}

columns_replacements = {
    "newCasesBySpecimenDate": "cases",
    "newCasesBySpecimenDateRollingRate": "rollingRate",
    "newCasesBySpecimenDateRollingSum": "rollingSum",
    "newCasesByPublishDate": "cases",
    "newCasesByPublishDateRollingRate": "rollingRate",
    "newCasesByPublishDateRollingSum": "rollingSum",
    "newDeaths28DaysByDeathDate": "deaths",
    "newDeaths28DaysByDeathDateRollingRate": "rollingRate",
    "newDeaths28DaysByDeathDateRollingSum": "rollingSum",
}


def get_latest_dataset():
    logging.info("Downloading latest main dataset")

    raw_data = get_latest_csv()

    d = read_csv(
        raw_data,
        usecols=[*index_metrics, ID_LABEL]
    )

    latest_dt = (
        d
        .loc[((d.areaCode.notnull()) & (d.areaType.notnull())), :]
        .sort_values(index_metrics, ascending=[True, True, False])
    )

    return latest_dt


def get_latest_count(category: DemographicsCategoryItem):
    count_path = COUNT_PATH.format(directory=category.directory)

    with StorageClient(path=count_path, **count_kws) as cli:
        data = loads(cli.download().readall().decode())

    data['lastUpdate'] = datetime.fromisoformat(data['lastUpdate'])

    return data


def get_latest_demographics(filepath, category: DemographicsCategoryItem):
    logging.info("Downloading latest demographic dataset")

    with StorageClient(container="pipeline", path=filepath) as cli:
        raw_data = StringIO(cli.download().readall().decode())

    data = read_csv(raw_data, usecols=[*index_metrics, *category.output_columns])
    data = (
        data
        .loc[((data.areaCode.notnull()) & (data.areaType.notnull())), :]
        .replace(label_replacements)
        .sort_values(index_metrics, ascending=[True, True, False])
    )

    count_path = COUNT_PATH.format(directory=category.directory)

    payload = dumps({
        "lastUpdate": datetime.utcnow().isoformat(),
        "count": data.loc[:, index_metrics].apply("|".join, axis=1).unique().size
    })

    with StorageClient(path=count_path, **count_kws) as cli:
        cli.upload(payload)

    return data


def format_struct(item: DataFrame, output_metric, output_columns):
    result = [
        item.loc[:, "hash"].unique()[0],
        {
            "$set": {
                output_metric: (
                    item
                    .loc[:, output_columns]
                    .to_dict(orient="records")
                )
            }
        }
    ]

    return dumps(result, separators=(",", ":"))


def format_struct_chunk(item, output_columns, output_metric, **kwargs):
    result = {
        **kwargs,
        "date": item["date"].unique()[0],
        output_metric: (
            item
            .loc[:, output_columns]
            .rename(columns=columns_replacements)
            .to_dict(orient="records")
        )
    }

    return result


def process_area(d, directory, output_columns, output_metric):
    values = (
        d
        .groupby("date")
        .apply(format_struct, output_columns=output_columns, output_metric=output_metric)
        .values
        .tolist()
    )

    try:
        area_code = d.areaCode.unique()[0]
        area_type = d.areaType.unique()[0]
    except AttributeError:
        return False

    file_path = JSONL_PATH.format(
        directory=directory,
        area_type=area_type,
        area_code=area_code
    )

    logging.info(f"Storing {file_path}")

    with StorageClient(container=STORAGE_CONTAINER, path=file_path) as client:
        client.upload(str.join("\n", values))

    enqueue_job(filepath=file_path, **queue_kws)

    return True


def process_area_chunk(d, directory, output_columns, output_metric, dataset_date):
    try:
        area_code = d.areaCode.unique()[0]
        area_type = d.areaType.unique()[0]
    except AttributeError:
        return False

    data = json_normalize(
        d
        .groupby("date")
        .apply(
            format_struct_chunk,
            output_columns=output_columns,
            output_metric=output_metric,
            areaType=area_type,
            areaCode=area_code
        )
        .values
    )

    file_path = FEATHER_PATH.format(
        directory=directory,
        date=dataset_date
    )

    filename = FEATHER_NAME.format(area_type=area_type, area_code=area_code)

    upload_chunk_feather(
        data=data,
        container=STORAGE_CONTAINER,
        dir_path=file_path,
        filename=filename
    )

    return True


def process(source_demographics, category: DemographicsCategoryItem, dataset_date):
    # logging.info("Starting the process")
    # data = get_latest_dataset()

    latest_dt = get_latest_demographics(source_demographics, category)
    logging.info("Datasets downloaded successfully")

    d = (
        latest_dt
        .dropna(subset=category.output_columns, how='all', axis='index')
    )
    logging.info("Datasets joined successfully")

    d.loc[:, category.output_columns] = (
        d[category.output_columns]
        .where(d[category.output_columns].notnull(), None)
    )
    logging.info("Starting generate DB outputs")

    d.groupby(["areaType", "areaCode"]).apply(
        # process_area,
        process_area_chunk,
        directory=category.directory,
        output_columns=category.output_columns,
        output_metric=category.output_metric,
        dataset_date=dataset_date
    )

    logging.info("All done")

    return True


async def process_by_age(source_demographics, category: Union[DemographicsCategoryItem, List[Any]]):
    """

    Parameters
    ----------
    source_demographics: Path to the demographics dataset.
    category: Data category
    """
    if isinstance(category, list):
        category = DemographicsCategoryItem(*category)

    # db_count = db_records_count()

    with StorageClient(container=STORAGE_CONTAINER, path="dispatch/total_records") as cli:
        data_count = int(cli.download().readall().decode())

    demographics_ts = get_timestamp_for(
        STORAGE_CONTAINER,
        source_demographics,
        raw=False,
        date_only=True
    )

    dataset_ts = get_release_timestamp(date_only=True)

    # if db_count != data_count and demographics_ts == dataset_ts:
    #     await sleep(30)
    #     enqueue_job(
    #         module='demographic_etl.db_processors',
    #         handler='process_by_age',
    #         source_demographics=source_demographics,
    #         category=category
    #     )
    #     return False
    # elif demographics_ts == dataset_ts:

    return process(source_demographics, category, demographics_ts)

    # else:
    #     raise RuntimeError(
    #         f"Datasets don't match - db_count: {db_count} / data_count: {data_count} | "
    #         f"demographics_ts: {demographics_ts} / dataset_ts: {data_count}"
    #     )


if __name__ == "__main__":
    # pass
    # enqueue_job(
    #     module='demographic_etl.db_processors',
    #     handler='process_by_age',
    #     source_demographics="demographic/publish_date_cases/by_age/in_waiting/stacked.csv",
    #     category=DemographicsCategory.publish_date_cases
    # )
    # enqueue_job(
    #     module='demographic_etl.db_processors',
    #     handler='process_by_age',
    #     source_demographics="demographic/specimen_date_cases/by_age/in_waiting/stacked.csv",
    #     category=DemographicsCategory.specimen_date_cases
    # )

#     from json import loads
#     from base64 import b64decode
#
#     q_data = loads(b64decode(b"""eyJtb2R1bGUiOiJkZW1vZ3JhcGhpY19ldGwuZGJfcHJvY2Vzc29ycy5kYl9wcm9jZXNzb3JzIiwi
# aGFuZGxlciI6InByb2Nlc3NfYnlfYWdlIiwiY29udGV4dCI6eyJzb3VyY2VfZGVtb2dyYXBoaWNz
# IjoiZGVtb2dyYXBoaWMvc3BlY2ltZW5fZGF0ZV9jYXNlcy9ieV9hZ2UvaW5fd2FpdGluZy9zdGFj
# a2VkLmNzdiIsImNhdGVnb3J5IjpbInNwZWNpbWVuX2RhdGUiLCJzcGVjaW1lbl9kYXRlX2Nhc2Vz
# IiwibmV3Q2FzZXNCeVNwZWNpbWVuRGF0ZSIsIm5ld0Nhc2VzQnlTcGVjaW1lbkRhdGVEZW1vZ3Jh
# cGhpY3MiLFsiYWdlIiwibmV3Q2FzZXNCeVNwZWNpbWVuRGF0ZSIsIm5ld0Nhc2VzQnlTcGVjaW1l
# bkRhdGVSb2xsaW5nU3VtIiwibmV3Q2FzZXNCeVNwZWNpbWVuRGF0ZVJvbGxpbmdSYXRlIl1dfX0=
# """).decode())
#     print(q_data)

    from asyncio import get_event_loop

    eloop = get_event_loop()

    # print(q_data['context'])
    # eloop.run_until_complete(process_by_age(**q_data['context']))
    # eloop.run_until_complete(process_by_age(**{
    #     'source_demographics': 'demographic/publish_date_cases/by_age/in_waiting/stacked.csv',
    #     'category': [
    #         'publish_date', 'publish_date_cases', 'newCasesByPublishDate', 'newCasesByPublishDateAgeDemographics',
    #         ['age', 'newCasesByPublishDate']
    #     ]
    # }))
    #

    # eloop.run_until_complete(process_by_age(**{
    #     'source_demographics': 'demographic/deaths_28days_death_date/by_age/in_waiting/stacked.csv',
    #     'category': [
    #         "deaths_28days_death_date", "deaths", "newDeaths28DaysByDeathDate", "newDeaths28DaysByDeathDateAgeDemographics",
    #         # 'publish_date', 'publish_date_cases', 'newCasesByPublishDate', 'newCasesByPublishDateAgeDemographics',
    #         [
    #             'age',
    #             "newDeaths28DaysByDeathDate",
    #             "newDeaths28DaysByDeathDateRollingSum",
    #             "newDeaths28DaysByDeathDateRollingRate"
    #          ]
    #     ]
    # }))

# coll_link = f'dbs/{Credentials.db_name}/colls/{Credentials.collection}/'
#
# def get_sproc_url(name, client, coll_link):
#     sp_query = f"select TOP 1 r._self from r WHERE r.id = '{name}'"
#     sprocs = list(client.QueryStoredProcedures(coll_link, sp_query))
#     return sprocs.pop(0)["_self"]
#
#
# def execute_upload(client, sproc_link, partition_key):
#     print(sproc_link)
#     for item in df:
#         print(item['hash'])
#
#         payload = [
#             item['hash'],
#             {
#                 "$set": {'newCasesBySpecimenDateDemographics': item['newCasesBySpecimenDateDemographics']}
#             }
#         ]
#
#         options = {
#             'setScriptLoggingEnabled': True,
#             'partitionKey': partition_value
#         }
#         sproc_response = client.ExecuteStoredProcedure(
#             sproc_link,
#             params=payload,
#             options=options
#         )
#
#         print(type(sproc_response), ':', sproc_response)
#
#
# sproc_url = get_sproc_url("updateById", client_conn, coll_link)
#
# execute_upload(client_conn, sproc_url, "")
