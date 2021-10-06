#!/usr/bin python3

# TODO: Deprecated - to be removed.

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import scandir
from os.path import join as join_path, split as split_path, abspath
from tempfile import gettempdir
from datetime import datetime
from zipfile import ZipFile
from itertools import repeat
import re
from datetime import timedelta
from asyncio import get_event_loop

# 3rd party:
from pandas import read_csv, json_normalize, DataFrame

# Internal:
try:
    from __app__.utilities.latest_data import get_latest_breakdowns_by_specimen_date, get_release_timestamp
    from __app__.db_etl.processors import normalise_records, homogenise_dates, calculate_age_rates
    from __app__.population import get_population_data
    from __app__.storage import StorageClient
    from __app__.fanout import enqueue_job
except ImportError:
    from utilities.latest_data import get_latest_breakdowns_by_specimen_date, get_release_timestamp
    from db_etl.processors import normalise_records, homogenise_dates, calculate_age_rates
    from population import get_population_data
    from storage import StorageClient
    from fanout import enqueue_job

try:
    from __app__.demographic_etl.db_processors.db_preprocessor import DemographicsCategory
except ImportError:
    from demographic_etl.db_processors.db_preprocessor import DemographicsCategory

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'create_specimen_date_dataset',
]


STACKED_ENDPOINT = "demographic/cases/specimenDate_ageDemographic-stacked.csv"
UNSTACKED_ENDPOINT = "demographic/cases/specimenDate_ageDemographic-unstacked.csv"

ARCHIVE_PATH = "demographic/specimen_date_cases/by_age/archive/{date}/{filename}.csv"

curr_dir, _ = split_path(abspath(__file__))
population_path = join_path(curr_dir, 'age-demographic-population.csv')


AREA_TYPES = {
    "nhs-country": "overview",
    "nhs-nation": "nation",
    "nhs-gov-region": "region",
    "nhs-local-authority": "utla",
    "nhs-lower-tier-local-authority": "ltla"
}

columns = [
    'newCasesBySpecimenDate0To4', 'newCasesBySpecimenDate5To9',
    'newCasesBySpecimenDate10To14', 'newCasesBySpecimenDate15To19',
    'newCasesBySpecimenDate20To24', 'newCasesBySpecimenDate25To29',
    'newCasesBySpecimenDate30To34', 'newCasesBySpecimenDate35To39',
    'newCasesBySpecimenDate40To44', 'newCasesBySpecimenDate45To49',
    'newCasesBySpecimenDate50To54', 'newCasesBySpecimenDate55To59',
    'newCasesBySpecimenDate60To64', 'newCasesBySpecimenDate65To69',
    'newCasesBySpecimenDate70To74', 'newCasesBySpecimenDate75To79',
    'newCasesBySpecimenDate80To84', 'newCasesBySpecimenDate85To89',
    'newCasesBySpecimenDate90+', 'newCasesBySpecimenDateUnknownAge'
]

upload_kws = dict(
    container="pipeline",
    content_type="text/csv; charset=utf-8",
    cache_control='no-store',
    tier="Cool"
)

temp_dir = gettempdir()

extraction_dir = join_path(temp_dir, 'specimen_date_files')
consolidated_path = join_path(temp_dir, 'specimen_data.csv')


with StorageClient(container="pipeline", path="assets/population.json") as client:
    population = get_population_data(client.download().readall().decode())


def load_column(col):
    included = ['areaCode', 'areaName', 'areaType', 'date']
    return col in columns or col in included


def get_column_name_repls():
    pattern = re.compile(r"(\d+)[^0-9+]*(\d+)?")

    column_names = dict()
    for item in columns:
        found = pattern.search(item)
        if found is None:
            continue

        if found.group(2) is not None:
            column_names[item] = f"{found.group(1)}_{found.group(2)}"
            continue

        column_names[item] = f"{found.group(1)}+"

    return column_names


def get_dtypes():
    dtypes = dict(zip(columns, repeat(float)))

    dtypes = {
        'areaCode': str,
        'areaName': str,
        'areaType': str,
        'date': str,
        **dtypes
    }

    return dtypes


def get_merged_dataset():
    data = get_latest_breakdowns_by_specimen_date()

    with ZipFile(data) as zip_data:
        zip_data.extractall(extraction_dir)

    with open(consolidated_path, 'w') as pointer:
        for index, item in enumerate(scandir(extraction_dir)):
            with open(item.path) as data_file:
                if index:
                    print(str.join("\n", data_file.read().split("\n")[1:]), file=pointer)
                print(data_file.read(), file=pointer)

    df = read_csv(
        consolidated_path,
        usecols=load_column,
        header=0,
        dtype=get_dtypes(),
        low_memory=False
    )

    return df


def get_prepped_population():
    population_dt = read_csv(population_path)

    population_dt = population_dt.pivot_table(
        index=["areaCode"],
        columns=["age"],
        values=["population"]
    )

    population_dt.columns = [item[1] for item in population_dt.columns]

    over_sixty_cols = {
        age for age in population_dt.columns
        if int(age.strip("+").split("_")[0]) >= 60
    }

    under_sixty_cols = set(population_dt.columns).difference(over_sixty_cols)

    population_dt.loc[:, "60+"] = population_dt.loc[:, over_sixty_cols].sum(axis=1)
    population_dt.loc[:, "0_59"] = population_dt.loc[:, under_sixty_cols].sum(axis=1)

    population_dt = population_dt.reset_index().melt(
        id_vars=['areaCode'],
        var_name="age",
        value_name='population'
    )

    return population_dt.groupby(["areaCode", "age"]).sum()


async def process():
    timestamp = (get_release_timestamp() - timedelta(days=5)).strftime("%Y-%m-%d")

    dt_init = get_merged_dataset()
    # dt_init.to_csv("dt_init.csv", index=False)
    # dt_init = read_csv("dt_init.csv", low_memory=True)

    # print(dt_init.head().to_string())
    col_names = get_column_name_repls()
    numeric_cols = [*list(col_names.values()), "newCasesBySpecimenDateUnknownAge"]

    over_sixty_cols = [
        col for col in numeric_cols
        if "unknown" not in col.lower() and int(col.strip("+").split("_")[0]) >= 60
    ]

    under_sixty_cols = [
        col for col in numeric_cols
        if col not in over_sixty_cols
    ]

    numeric_cols = [*numeric_cols, "60+", "0_59"]

    dt_init.rename(columns=col_names, inplace=True)

    dt = (
        dt_init
        .sort_values(["areaType", "areaCode", "date"])
        .assign(**{
            "60+": dt_init.loc[:, over_sixty_cols].sum(axis=1),
            "0_59": dt_init.loc[:, under_sixty_cols].sum(axis=1),
        })
        .replace(AREA_TYPES)
        .pipe(homogenise_dates)
        .pipe(normalise_records, zero_filled=list(numeric_cols))
        .drop(columns=['areaNameLower'])
        .dropna(subset=numeric_cols)
        .reset_index(drop=True)
        .rename(columns={"newCasesBySpecimenDateUnknownAge": "unassigned"})
        .melt(
            id_vars=['areaType', 'areaCode', 'areaName', 'date'],
            var_name="age",
            value_name='newCasesBySpecimenDate'
        )
        .pipe(
            calculate_age_rates,
            population_data=get_prepped_population(),
            max_date=timestamp,
            rolling_rate=['newCasesBySpecimenDate']
        )
    )

    dt = dt.loc[dt.date <= timestamp, :]

    file_timestamp = datetime.now().strftime("%Y-%m-%d")

    stacked_file = dt.to_csv(float_format="%.12g", index=False)

    with StorageClient(
        path=ARCHIVE_PATH.format(date=file_timestamp, filename="stacked"),
        content_disposition=f'attachment; filename="specimenDate_ageDemographic-stacked.csv"',
        **upload_kws
    ) as cli:
        cli.upload(stacked_file)

    stacked_filename = NEXT_DEPLOYMENT_PATH.format(filename="stacked")
    with StorageClient(
        path=stacked_filename,
        content_disposition=f'attachment; filename="specimenDate_ageDemographic-stacked.csv"',
        **upload_kws
    ) as cli:
        cli.upload(stacked_file)

    # with StorageClient(
    #     path="demographic/cases/specimenDate_ageDemographic-stacked.csv",
    #     content_disposition=f'attachment; filename="specimenDate_ageDemographic-stacked.csv"',
    #     **upload_kws
    # ) as cli:
    #     cli.upload(stacked_file)

    dt_unstacked = (
        dt
        .pivot_table(
            index=["areaType", "areaCode", "areaName", "date"],
            columns=["age"],
            values=[
                "newCasesBySpecimenDate",
                "newCasesBySpecimenDateRollingSum",
                "newCasesBySpecimenDateRollingSum"
            ]
        )
    )

    dt_unstacked.columns = dt_unstacked.columns.map('{0[0]}-{0[1]}'.format)
    unstacked_file = dt_unstacked.to_csv(float_format="%.12g", index=True)

    with StorageClient(
        path=ARCHIVE_PATH.format(date=file_timestamp, filename="unstacked"),
        content_disposition=f'attachment; filename="specimenDate_ageDemographic-unstacked.csv"',
        **upload_kws
    ) as cli:
        cli.upload(unstacked_file)

    with StorageClient(
            path=NEXT_DEPLOYMENT_PATH.format(filename="unstacked"),
            content_disposition=f'attachment; filename="specimenDate_ageDemographic-unstacked.csv"',
            **upload_kws
    ) as cli:
        cli.upload(unstacked_file)

    # event_loop = get_event_loop()
    enqueue_job(
        module='demographic_etl.db_processors',
        handler='process_by_age',
        source_demographics=stacked_filename,
        category=DemographicsCategory.specimen_date_cases
    )

    return True


async def create_specimen_date_dataset():
    event_loop = get_event_loop()
    event_loop.create_task(process())

    return True

    # with StorageClient(
    #     path="demographic/cases/specimenDate_ageDemographic-unstacked.csv",
    #     content_disposition=f'attachment; filename="specimenDate_ageDemographic-unstacked.csv"',
    #     **upload_kws
    # ) as cli:
    #     cli.upload(dt_unstacked.to_csv(float_format="%.12g", index=True))


if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(create_specimen_date_dataset())
