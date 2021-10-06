#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
import re
from asyncio import get_event_loop
from datetime import datetime

# 3rd party:
from pandas import read_csv
from azure.functions import ServiceBusMessage

# Internal:
try:
    from __app__.utilities.latest_data import get_latest_breakdown
    from __app__.db_etl.processors import normalise_records, homogenise_dates, calculate_rates
    from __app__.population import get_population_data
    from __app__.storage import StorageClient
    from __app__.fanout import enqueue_job
    from __app__.demographic_etl.categories import DemographicsCategory
except ImportError:
    from utilities.latest_data import get_latest_breakdown
    from db_etl.processors import normalise_records, homogenise_dates, calculate_rates
    from population import get_population_data
    from storage import StorageClient
    from fanout import enqueue_job
    from demographic_etl.categories import DemographicsCategory

try:
    from .constants import NEXT_DEPLOYMENT_PATH
except ImportError:
    from publish_date_demographics.constants import NEXT_DEPLOYMENT_PATH

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main",
]


with StorageClient(container="pipeline", path="assets/population.json") as client:
    population = get_population_data(client.download().readall().decode())

AREA_TYPE_ENUM = {
    "overview": 0,
    "nation": 1,
    "region": 2,
    "nhsRegion": 3,
    "utla": 4,
    "ltla": 5
}

AGE_BRACKET_ENUM = {
    "unassigned": -99,
    "overall": -1,
    "00_04": 0,
    "05_09": 1,
    "10_14": 2,
    "15_19": 3,
    "20_24": 4,
    "25_29": 5,
    "30_34": 6,
    "35_39": 7,
    "40_44": 8,
    "45_49": 9,
    "50_54": 10,
    "55_59": 11,
    "60_64": 12,
    "65_69": 13,
    "70_74": 14,
    "75_79": 15,
    "80_84": 16,
    "85_89": 17,
    "90+": 18,
    "85+": 19
}

CATEGORY_ENUM = {
    "cases": 0,
    "deaths": 1
}

ARCHIVE_PATH = "demographic/publish_date_cases/by_age/archive/{date}/{filename}.csv"

upload_kws = dict(
    container="pipeline",
    content_type="text/csv; charset=utf-8",
    cache_control='no-store',
    tier="Cool"
)


async def process():
    raw_data = get_latest_breakdown()
    data = read_csv(raw_data)

    data.replace(AGE_BRACKET_ENUM, inplace=True)
    data.areaType.replace(dict(zip(AREA_TYPE_ENUM.values(), AREA_TYPE_ENUM)), inplace=True)
    data.age.fillna(AGE_BRACKET_ENUM["unassigned"], inplace=True)
    data.age = data.age.astype(int)

    data.age = data.age.replace(dict(zip(AGE_BRACKET_ENUM.values(), AGE_BRACKET_ENUM.keys())))

    numeric_cols = [key for key, value in AGE_BRACKET_ENUM.items() if value >= 0]

    over_sixty_cols = [
        col for col in numeric_cols
        if "unknown" not in col.lower() and int(col.strip("+").split("_")[0]) >= 60
    ]

    under_sixty_cols = [
        col for col in numeric_cols
        if col not in over_sixty_cols
    ]

    numeric_cols = [*numeric_cols, "60+", "0_59"]

    data = (
        data
        .loc[:, data.columns[~data.columns.isin(["overall", "unassigned"])]]
        .loc[:, ['areaType', 'areaCode', 'areaName', 'date', 'age', 'newCases']]
        .sort_values(['areaType', 'areaCode', 'date'], ascending=[True, True, True])
        .pivot_table(
            columns=['age'],
            index=['areaType', 'areaCode', 'areaName', 'date'],
            aggfunc=lambda x: x.max()
        )
        .sort_values(['areaType', 'areaCode', 'date'], ascending=[True, True, True])
    )

    data.columns = [col[1] for col in data.columns]
    data = data.dropna(subset=set(numeric_cols).intersection(data.columns), how="all")

    over_sixty_cols = {item for item in over_sixty_cols}
    under_sixty_cols = {item for item in under_sixty_cols}

    data = data.assign(**{
        "60+": data.loc[:, over_sixty_cols.intersection(data.columns)].sum(axis=1),
        "00_59": data.loc[:, under_sixty_cols.intersection(data.columns)].sum(axis=1),
    })

    data.columns = [
        re.sub(r'(0(\d))', r'\2', col) if re.search(r'(0(\d))', col) else col
        for col in data.columns
    ]

    data.reset_index(inplace=True)
    data = data.loc[~data.areaCode.str.startswith("S"), :].dropna(axis=1, how="all")

    # with StorageClient(
    #     container="downloads",
    #     path="demographic/cases/publish_date-latest.csv",
    #     content_type="text/csv; charset=utf-8",
    #     content_disposition=f'attachment; filename="publish_date-latest.csv"',
    #     cache_control='no-store',
    #     tier="Hot"
    # ) as cli:
    #     cli.upload(data.to_csv(float_format="%.12g", index=False))

    file_timestamp = datetime.now().strftime("%Y-%m-%d")
    unstacked_file = data.to_csv(float_format="%.12g", index=False)

    with StorageClient(
            path=ARCHIVE_PATH.format(filename='unstacked', date=file_timestamp),
            **upload_kws) as cli:
        cli.upload(unstacked_file)

    with StorageClient(
            path=NEXT_DEPLOYMENT_PATH.format(filename='unstacked'),
            **upload_kws) as cli:
        cli.upload(unstacked_file)

    stacked = (
        data
        .melt(
            id_vars=['areaType', 'areaCode', 'areaName', 'date'],
            var_name='age',
            value_name='newCasesByPublishDate'
        )
        .reset_index(drop=True)
        .to_csv(float_format="%.12g", index=False)
    )

    with StorageClient(
            path=ARCHIVE_PATH.format(filename='stacked', date=file_timestamp),
            **upload_kws) as cli:
        cli.upload(stacked)

    stacked_filename = NEXT_DEPLOYMENT_PATH.format(filename='stacked')
    with StorageClient(path=stacked_filename, **upload_kws) as cli:
        cli.upload(stacked)

    enqueue_job(
        module='demographic_etl.db_processors',
        handler='process_by_age',
        source_demographics=stacked_filename,
        category=DemographicsCategory.publish_date_cases
    )

    # event_loop = get_event_loop()
    # task = process_by_age(stacked_filename, DemographicsCategory.publish_date_cases)
    # event_loop.create_task(task)

    return True


async def main(message: ServiceBusMessage):
    logging.info("Triggered publish date demographics processor")
    event_loop = get_event_loop()
    event_loop.create_task(process())

    # return True


colorscale = [
    "#e0e543",
    "#74bb68",
    "#399384",
    "#2067AB",
    "#12407F",
    "#53084A"
]


if __name__ == '__main__':
    loop = get_event_loop()
    loop.run_until_complete(main(""))
