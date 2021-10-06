#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       20 Oct 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from io import StringIO, BytesIO
from datetime import datetime
from json import loads
from pathlib import Path

# 3rd party:
from pandas import DataFrame, read_csv

# Internal: 
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

try:
    from .generic_types import PopulationData
except ImportError:
    from utilities.generic_types import PopulationData

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_latest_csv',
    'get_release_timestamp',
    'get_published_timestamp',
    'get_website_timestamp',
    'get_latest_breakdown',
    'get_latest_breakdowns_by_specimen_date',
    'get_archive_dates',
    'get_timestamp_for',
    'get_storage_file',
    'get_population_data',
    'get_latest_msoa_data',
    # 'get_demographics_population_data'
]

base_dir = Path(__file__).parent.resolve()
demographics_population_path = base_dir.joinpath(
    'statics',
    'supplements',
    'age-demographic-population.csv'
)


def get_storage_file(container, path):
    with StorageClient(container, path) as client:
        dt = client.download().readall().decode()

    return dt


def get_release_timestamp(date_only=True, raw=False):
    with StorageClient("pipeline", "info/latest_available") as client:
        dt = client.download().readall().decode()

    if raw:
        return dt

    if date_only:
        return datetime.strptime(dt[:24] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ").date()

    return datetime.strptime(dt[:24] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ")


def get_published_timestamp(raw=False):
    with StorageClient("pipeline", "info/latest_published") as client:
        dt = client.download().readall().decode()

    if raw:
        return dt

    return datetime.strptime(dt[:24] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ")


def get_website_timestamp():
    with StorageClient("publicdata", "assets/dispatch/website_timestamp") as client:
        dt = client.download().readall().decode()

    return datetime.strptime(dt[:24] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ")


def get_timestamp_for(container, path, raw=True, date_only=False):
    with StorageClient(container=container, path=path) as cli:
        timestamp = max(cli, key=lambda x: x['last_modified'])['last_modified']

    if raw and not date_only:
        return timestamp.strftime(r"%Y-%m-%dT%H:%M:%S.%fZ")

    if raw and date_only:
        return timestamp.strftime(r"%Y-%m-%d")

    if not raw and not date_only:
        return timestamp

    if not raw and date_only:
        return timestamp.date()


def get_latest_csv():
    with StorageClient(container="pipeline", path=f"archive/processed") as cli:
        filtered_names = filter(lambda x: x['name'].endswith("csv"), cli)
        name = max(filtered_names, key=lambda x: x['last_modified'])['name']
        cli.path = name
        data = cli.download().readall().decode()

    return StringIO(data)


def get_latest_breakdown():
    with StorageClient(container="rawbreakdowndata",
                       path="publish_date/daily_cases_by_pub_with_demography_") as cli:
        filtered_names = filter(lambda x: x['name'].endswith("csv"), cli)
        name = max(filtered_names, key=lambda x: x['last_modified'])['name']
        cli.path = name
        data = cli.download().readall().decode()

    return StringIO(data)


def get_latest_breakdowns_by_specimen_date():
    with StorageClient(container="rawbreakdowndata",
                       path="specimen_date/daily_cases_") as cli:
        filtered_names = filter(lambda x: x['name'].endswith("zip"), cli)
        name = max(filtered_names, key=lambda x: x['last_modified'])['name']
        cli.path = name
        data = cli.download().readall()

    return BytesIO(data)


def get_archive_dates():
    with StorageClient(container="publicdata",
                       path="assets/dispatch/dates.json") as cli:
        data = cli.download().readall().decode()

    return loads(data)


def get_population_data() -> PopulationData:
    try:
        from __app__.population import get_population_data as process_data
    except ImportError:
        from population import get_population_data as process_data

    with StorageClient(container="pipeline",
                       path="assets/population.json") as cli:
        data = cli.download().readall().decode()

    return process_data(data)


# def get_demographics_population_data():
#     population_dt = read_csv(demographics_population_path.resolve().absolute())
#
#     population_dt = population_dt.pivot_table(
#         index=["areaCode"],
#         columns=["age"],
#         values=["population"]
#     )
#
#     population_dt.columns = [item[1] for item in population_dt.columns]
#
#     over_sixty_cols = {
#         age for age in population_dt.columns
#         if int(age.strip("+").split("_")[0]) >= 60
#     }
#
#     under_sixty_cols = set(population_dt.columns).difference(over_sixty_cols)
#
#     population_dt.loc[:, "60+"] = population_dt.loc[:, over_sixty_cols].sum(axis=1)
#     population_dt.loc[:, "0_59"] = population_dt.loc[:, under_sixty_cols].sum(axis=1)
#
#     population_dt = population_dt.reset_index().melt(
#         id_vars=['areaCode'],
#         var_name="age",
#         value_name='population'
#     )
#
#     return population_dt.groupby(["areaCode", "age"]).sum()


def get_latest_msoa_data() -> DataFrame:
    with StorageClient("pipeline", "assets/msoa_pop2019.csv") as client:
        population_io = StringIO(client.download().readall().decode())

    msoa_population = (
        read_csv(population_io, low_memory=False)
        .rename(columns=["areaCode", "population"], inplace=True)
        .set_index("areaCode", inplace=True)
    )

    with StorageClient(container="rawsoadata", path="daily_") as cli:
        csv_files = filter(lambda x: x['name'].endswith("csv"), cli)
        cli.path = max(csv_files, key=lambda x: x['name'])['name']
        data_io = StringIO(
            cli
            .download()
            .readall()
            .decode()
            .replace("nhs-msoa", "msoa")
            .replace("-99", "")
        )

    raw_data = read_csv(data_io, low_memory=False, usecols=lambda x: x != "areaName")

    raw_data = raw_data.join(msoa_population, on=["areaCode"])

    return raw_data
