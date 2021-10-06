#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import (
    NamedTuple, Any, NoReturn,
    Union, Dict, List, Tuple
)
from json import dumps, loads
from os import getenv, makedirs
from os.path import split as split_path
from datetime import datetime, timedelta
from copy import deepcopy
from http import HTTPStatus

# 3rd party:
from azure.functions import HttpRequest, HttpResponse
from azure.storage.blob import BlobClient, BlobType, ContentSettings, StandardBlobTier

from pandas import DataFrame, to_datetime, json_normalize

# Internal
try:
    from __app__.utilities import func_logger
    from __app__.population import get_population_data, PopulationData
    from __app__.JsonOutput import produce_json
    # from __app__.og_image import generate_og_images
    from __app__.fanout import enqueue_job
    from __app__.daily_report import generate_report
    from __app__.storage import StorageClient
    from .token import generate_token
    from .processors import (
        homogenise_dates, normalise_records,
        calculate_pair_summations, calculate_by_adjacent_column,
        calculate_rates, generate_row_hash, change_by_sum,
        ratio_to_percentage, trim_end, match_area_names
    )
    from .db_uploader.chunk_ops import (
        save_chunk_feather, upload_chunk_feather
    )
except ImportError:
    from utilities import func_logger
    from fanout import enqueue_job
    # from og_image import generate_og_images
    from storage import StorageClient
    from population import get_population_data, PopulationData
    from JsonOutput import produce_json
    from daily_report import generate_report
    from db_etl.token import generate_token
    from db_etl.processors import (
        homogenise_dates, normalise_records,
        calculate_pair_summations, calculate_by_adjacent_column,
        calculate_rates, generate_row_hash, change_by_sum,
        ratio_to_percentage, trim_end, match_area_names
    )
    from db_etl.db_uploader.chunk_ops import (
        save_chunk_feather, upload_chunk_feather
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")

# DEBUG = True and not ENVIRONMENT == "PRODUCTION"
DEBUG = False

RANDOMISE = False

VALUE_COLUMNS = (
    # "unoccupiedOSBeds",  # Deprecated
    # "covidOccupiedOSBeds",  # Deprecated
    # "nonCovidOccupiedOSBeds",  # Deprecated
    # "unoccupiedNIVBeds",  # Deprecated
    # "covidOccupiedOtherBeds",  # Deprecated
    # "nonCovidOccupiedOtherBeds",  # Deprecated
    # "cumPillarTwoPeopleTestedByPublishDate",  # Currently excluded from the data.
    # "plannedPillarOneCapacityByPublishDate",  # Deprecated
    # "newDischarges",  # Deprecated
    # "totalOtherBeds",  # Deprecated
    # "plannedPillarTwoCapacityByPublishDate",  # Deprecated
    # "suspectedCovidOccupiedOSBeds",  # Deprecated
    # "plannedPillarThreeCapacityByPublishDate",  # Deprecated
    # "nonCovidOccupiedNIVBeds",  # Deprecated
    # "totalOSBeds",  # Deprecated
    # "newPillarOnePeopleTestedByPublishDate",  # Currently excluded from the data.
    # "newPillarTwoPeopleTestedByPublishDate",  # Currently excluded from the data.
    # "cumPillarFourPeopleTestedByPublishDate",  # Currently excluded from the data.
    # "plannedPillarFourCapacityByPublishDate",  # Renamed
    # "covidOccupiedNIVBeds",  # Deprecated
    # "unoccupiedOtherBeds",  # Deprecated
    # "cumPillarOnePeopleTestedByPublishDate",  # Currently excluded from the data.
    # "cumDischarges",  # Deprecated
    # "cumDischargesByAge",  # Deprecated
    # "totalNIVBeds",  # Deprecated
    # "newPillarFourPeopleTestedByPublishDate",  # Currently excluded from the data.
    # "suspectedCovidOccupiedNIVBeds",  # Deprecated
    # "totalBeds",  # Deprecated
    # "suspectedCovidOccupiedOtherBeds",  # Deprecated
    # "capacityPillarOneTwoFour",  # plannedPillarOneTwoFourCapacityByPublishDate
    # "newPillarOneTwoFourTestsByPublishDate",  # Deprecated

    "femaleNegatives",
    "maleCases",
    "previouslyReportedCumCasesBySpecimenDate",
    "cumCasesByPublishDate",
    "newAdmissionsByAge",
    "cumPillarTwoTestsByPublishDate",
    "newCasesBySpecimenDate",
    "changeInNewCasesBySpecimenDate",

    "totalMVBeds",
    "unoccupiedMVBeds",
    "covidOccupiedMVBeds",
    "suspectedCovidOccupiedMVBeds",
    "nonCovidOccupiedMVBeds",

    "changeInCumCasesBySpecimenDate",
    "cumPillarThreeTestsByPublishDate",
    "cumTestsByPublishDate",
    "previouslyReportedNewCasesBySpecimenDate",
    "newPillarOneTestsByPublishDate",
    "plannedCapacityByPublishDate",
    "newPeopleTestedByPublishDate",
    "newAdmissions",
    "newPillarFourTestsByPublishDate",
    "cumCasesBySpecimenDate",
    "cumAdmissions",
    "cumNegativesBySpecimenDate",
    "newCasesByPublishDate",
    "femaleCases",
    "cumAdmissionsByAge",
    "newPillarThreeTestsByPublishDate",
    "cumPillarFourTestsByPublishDate",
    "newTestsByPublishDate",
    "newPillarTwoTestsByPublishDate",
    "newNegativesBySpecimenDate",
    "cumPeopleTestedByPublishDate",
    "cumPillarOneTestsByPublishDate",
    "maleNegatives",
    "hospitalCases",

    "newDeathsByPublishDate",
    "newDeathsByDeathDate",
    "cumDeathsByPublishDate",
    "cumDeathsByDeathDate",
    "maleDeaths",
    "femaleDeaths",

    "femaleDeaths28Days",
    "femaleDeaths60Days",
    "maleDeaths28Days",
    "maleDeaths60Days",

    "newDeathsByDeathDate",
    "cumDeathsByDeathDate",

    "newDeathsByPublishDate",
    "cumDeathsByPublishDate",

    "newDeaths28DaysByDeathDate",
    "cumDeaths28DaysByDeathDate",

    "newDeaths28DaysByPublishDate",
    "cumDeaths28DaysByPublishDate",

    "newDeaths60DaysByDeathDate",
    "cumDeaths60DaysByDeathDate",

    "newDeaths60DaysByPublishDate",
    "cumDeaths60DaysByPublishDate",

    "newOnsDeathsByRegistrationDate",
    "cumOnsDeathsByRegistrationDate",

    "newPillarOneTwoTestsByPublishDate",
    "capacityPillarOne",
    "capacityPillarTwo",
    "capacityPillarOneTwo",
    "capacityPillarThree",
    "capacityPillarFour",

    "cumPillarOneTwoTestsByPublishDate",

    "newPCRTestsByPublishDate",
    "cumPCRTestsByPublishDate",
    "plannedPCRCapacityByPublishDate",
    "plannedAntibodyCapacityByPublishDate",
    "newAntibodyTestsByPublishDate",
    "cumAntibodyTestsByPublishDate",

    'transmissionRateMin',
    'transmissionRateMax',
    'transmissionRateGrowthRateMin',
    'transmissionRateGrowthRateMax',

    'alertLevel',

    'newLFDTests',
    'cumLFDTests',
    'newVirusTests',
    'cumVirusTests',

    "newOnsCareHomeDeathsByRegistrationDate",
    "cumOnsCareHomeDeathsByRegistrationDate",

    "uniqueCasePositivityBySpecimenDateRollingSum",
    "uniquePeopleTestedBySpecimenDateRollingSum",

    *(
        (
            'newPeopleReceivingFirstDose',
            'cumPeopleReceivingFirstDose',
            'newPeopleReceivingSecondDose',
            'cumPeopleReceivingSecondDose',
        ) if ENVIRONMENT == 'DEVELOPMENT'
        else tuple()
    ),

    'cumWeeklyNsoDeathsByRegDate',
    'newWeeklyNsoDeathsByRegDate',
    'cumWeeklyNsoCareHomeDeathsByRegDate',
    'newWeeklyNsoCareHomeDeathsByRegDate',
)


WITH_CUMULATIVE = []

RENAME = {
    # from : to
    # "plannedPillarOneTwoFourCapacityByPublishDate": "capacityPillarOneTwoFour",
    "plannedPillarOneTwoCapacityByPublishDate": "capacityPillarOneTwo",
    "plannedPillarThreeCapacityByPublishDate": "capacityPillarThree",
    "plannedPillarOneCapacityByPublishDate": "capacityPillarOne",
    "plannedPillarTwoCapacityByPublishDate": "capacityPillarTwo",
    "plannedPillarFourCapacityByPublishDate": "capacityPillarFour",
    'newLFDTestsBySpecimenDate': 'newLFDTests',
    'cumLFDTestsBySpecimenDate': 'cumLFDTests',
    'newVirusTestsByPublishDate': 'newVirusTests',
    'cumVirusTestsByPublishDate': 'cumVirusTests',
}

FULL_VALUE_COLUMNS = {
    # Existing
    *VALUE_COLUMNS,

    # Calculated as a part of the ETL
    "cumPeopleTestedBySpecimenDate",
    "cumDeathsByPublishDateRate",
    "cumDeathsByDeathDateRate",
    'newCasesBySpecimenDateRollingRate',
    'newDeathsByDeathDateRollingRate',
    'cumPeopleTestedBySpecimenDate',
    'newPeopleTestedBySpecimenDate',
}


CATEGORY_LABELS = (
    "overview",
    "nations",
    "regions",
    "nhsRegions",
    "nhsTrusts",
    "utlas",
    "ltlas"
)


DATE_COLUMN = "date"

SORT_OUTPUT_BY = {
    'by': ["areaType", "areaCode", "date"],
    'ascending': [True, False, False]
}


DAILY_RECORD_LABELS = {
    "totalCases": "totalLabConfirmedCases",
    "newCases": "dailyLabConfirmedCases",
}


NEGATIVE_TO_ZERO = [
    "newCasesByPublishDate",
    "newDeathsByPublishDate",
    "newDeaths28DaysByPublishDate",
    "newDeaths60DaysByPublishDate",
]


# Fields from which the population adjusted rates are
# calculated using `PopulationData.ageSexBroadBreakdown`.
POPULATION_ADJUSTED_RATES_BROAD = {
    'newAdmissionsByAge',
    'cumAdmissionsByAge',
    # 'cumDischargesByAge'
}


# Fields from which the population adjusted rates are
# calculated using `PopulationData.ageSex5YearBreakdown`.
POPULATION_ADJUSTED_RATES_5YEAR = {
    'maleCases',
    'femaleCases',
    'maleNegatives',
    'femaleNegatives',
    'maleDeaths',
    'femaleDeaths',
    "femaleDeaths28Days",
    "femaleDeaths60Days",
    "maleDeaths28Days",
    "maleDeaths60Days",
}


INCIDENCE_RATE_FIELDS = {
    "newCasesBySpecimenDate",
    "cumCasesBySpecimenDate",
    "cumCasesByPublishDate",
    "cumPeopleTestedByPublishDate",
    "cumAdmissions",
    # "cumDischarges",

    "newDeathsByDeathDate",
    "newDeaths28DaysByDeathDate",
    "newDeaths60DaysByDeathDate",

    "cumDeathsByDeathDate",
    "cumDeathsByPublishDate",

    "cumDeaths28DaysByDeathDate",
    "cumDeaths28DaysByPublishDate",

    "cumDeaths60DaysByDeathDate",
    "cumDeaths60DaysByPublishDate",

    "cumOnsDeathsByRegistrationDate",
}


ROLLING_RATE = {
    "newCasesBySpecimenDate",
    "newCasesByPublishDate",
    "newDeathsByDeathDate",
    "newDeaths28DaysByDeathDate",
    "newDeaths60DaysByDeathDate",
    "newAdmissions"
}


# These get incidence rate calculated.
DERIVED_FROM_NESTED = {
    # "femalePeopleTested": ("femaleCases", "femaleNegatives"),
    # "malePeopleTested": ("maleCases", "maleNegatives")
}


DERIVED_BY_SUMMATION = {
    "cumPeopleTestedBySpecimenDate":
        ("cumCasesBySpecimenDate", "cumNegativesBySpecimenDate"),

    "newPeopleTestedBySpecimenDate":
        ("newCasesBySpecimenDate", "newNegativesBySpecimenDate")
}


DERIVED_BY_MAX_OF_ADJACENT_COLUMN = {
    "cumPeopleTestedByPublishDate": "cumPeopleTestedBySpecimenDate",
    "cumCasesByPublishDate": "cumCasesBySpecimenDate",
    "cumDeathsByPublishDate": "cumDeathsByDeathDate",
}


FILL_WITH_ZEROS = {
    *ROLLING_RATE,
    'newDeathsByPublishDate',
    'newDeathsByDeathDate',
    'newCasesBySpecimenDate',
    'newNegativesBySpecimenDate',
    'newCasesBySpecimenDate',
    'newNegativesBySpecimenDate',
    'newLFDTests',
}


START_WITH_ZERO = {
    'cumDeathsByPublishDate',
    'cumDeathsByDeathDate',
    'cumCasesBySpecimenDate',
    'cumNegativesBySpecimenDate',
    'cumLFDTests',
    'cumDeaths28DaysByDeathDate',
    'cumDeaths60DaysByDeathDate',
}


AREA_TYPE_NAMES = {
    'nations': 'nation',
    'nhsTrusts': 'nhsTrust',
    'utlas': 'utla',
    'ltlas': 'ltla',
    'nhsRegions': 'nhsRegion',
    'regions': 'region',
    'uk': 'overview'
}


RATIO2PERCENTAGE = [
    "uniqueCasePositivityBySpecimenDateRollingSum",
]


TRIM_END = {
    "days_to_trim": 5,
    "metrics": (
        "uniqueCasePositivityBySpecimenDateRollingSum",
        "uniquePeopleTestedBySpecimenDateRollingSum"
    )
}


RATE_PER_POPULATION_FACTOR = 100_000  # Per resident population


RATE_PRECISION = 1  # Decimal places


SUM_CHANGE_DIRECTION = {
    'newCasesBySpecimenDate',
    'newCasesByPublishDate',
    'newAdmissions',
    'newDeaths28DaysByPublishDate',
    'newPCRTestsByPublishDate',
    'newVirusTests'
}


# Get incidence rate calculated.
OUTLIERS = [
    'maleCases',
    'femaleCases',
    'maleNegatives',
    'femaleNegatives',
    'newAdmissionsByAge',
    'cumAdmissionsByAge',
    # 'cumDischargesByAge',
    'maleDeaths',
    'femaleDeaths',
    "femaleDeaths28Days",
    "femaleDeaths60Days",
    "maleDeaths28Days",
    "maleDeaths60Days",
]


WELSH_LA = [
    "Wales",
    "Isle of Anglesey",
    "Gwynedd",
    "Conwy",
    "Denbighshire",
    "Flintshire",
    "Wrexham",
    "Ceredigion",
    "Pembrokeshire",
    "Carmarthenshire",
    "Swansea",
    "Neath Port Talbot",
    "Bridgend",
    "Vale of Glamorgan",
    "Cardiff",
    "Rhondda Cynon Taf",
    "Caerphilly",
    "Blaenau Gwent",
    "Torfaen",
    "Monmouthshire",
    "Newport",
    "Powys",
    "Merthyr Tydfil"
]


CONTAINER_NAME = getenv("StorageContainerName")
STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
RAW_DATA_CONTAINER = "rawdbdata"


class ProcessedData(NamedTuple):
    data_table: DataFrame
    sample_row: str
    csv: str
    timestamp: str


class InternalProcessor(NamedTuple):
    json: str


class GeneralProcessor(NamedTuple):
    data: DataFrame
    json_extras: Any


def download_file(container: str, path: str) -> bytes:
    client = BlobClient.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name=container,
        blob_name=path
    )

    data = client.download_blob()
    return data.readall()


class TestOutput:
    def __init__(self, path):
        self.path = path

        if DEBUG:
            self.path = f"test/v2/{path}"

    def set(self, data: Union[str, bytes], content_type: str = "application/json",
            cache: str = "no-store") -> NoReturn:
        mode = "w" if isinstance(data, str) else "wb"

        dir_path, _ = split_path(self.path)
        makedirs(dir_path, exist_ok=True)

        with open(self.path, mode=mode) as output_file:
            print(data, file=output_file)


class MainOutput(TestOutput):
    def __init__(self, *args, chunks=False, **kwargs):
        super().__init__(*args, **kwargs)

        self.chunks = chunks

        # if not chunks:
        self.client = BlobClient.from_connection_string(
            conn_str=STORAGE_CONNECTION_STRING,
            container_name=CONTAINER_NAME,
            blob_name=self.path,
            **kwargs
        )

    def set(self, data: Union[str, bytes], content_type: str = "application/json",
            cache: str = "no-store") -> NoReturn:
        self.client.upload_blob(
            data,
            blob_type=BlobType.BlockBlob,
            content_settings=ContentSettings(
                content_type=content_type,
                cache_control=cache
            ),
            overwrite=True,
            standard_blob_tier=StandardBlobTier.Cool
        )


def repl_values(dt: DataFrame, field, index_by):
    """

    Parameters
    ----------
    dt
    field
    index_by

    Returns
    -------

    """
    dt_tmp = dt.loc[~dt[field].isna(), [field, index_by]].drop_duplicates()
    dt_tmp.index = dt_tmp.loc[:, index_by]
    dt.loc[:, field] = dt_tmp.loc[dt.loc[:, index_by], field].values
    return dt


def get_population_set(population_data, area_code, category):
    """

    Parameters
    ----------
    population_data
    area_code
    category

    Returns
    -------

    """
    if category in POPULATION_ADJUSTED_RATES_BROAD:
        population_type = "total"
        population_set = population_data.ageSexBroadBreakdown[population_type][area_code]

    elif category in POPULATION_ADJUSTED_RATES_5YEAR:
        population_type = "male"

        if "female" in category.lower():
            population_type = "female"

        population_set = population_data.ageSex5YearBreakdown[population_type][area_code]
    else:
        population_set = dict()

    return population_set


def process_outlier(data, population_set):
    """

    Parameters
    ----------
    data
    population_set

    Returns
    -------

    """
    content = list()

    for date in set(map(lambda x: x['date'], data)):
        tmp_item = {
            "date": date,
            "value": list()
        }

        for value in filter(lambda d: d["date"] == date, data):
            tmp_value = deepcopy(value)
            del tmp_value["date"]

            if population_set:
                tmp_value["rate"] = round(
                    tmp_value["value"] / population_set[tmp_value["age"]] *
                    RATE_PER_POPULATION_FACTOR,
                    1
                )

            tmp_item["value"].append(tmp_value)

        content.append(tmp_item)

    return deepcopy(content)


@func_logger("area type adjustment")
def adjust_area_types(data, replacements: Dict[str, str]) -> DataFrame:
    data.areaType = data.loc[:, ['areaType']].replace(replacements)

    return data


def get_sample_json(data: DataFrame) -> str:
    """
    Produces a sample JSON from the data, containing a single
    row for England on ``date.max() - timedelta(days=1)``.
    """
    sample_data = datetime.strptime(data.date.max(), "%Y-%m-%d")
    sample_data -= timedelta(days=1)

    non_integer = {
        # Contain floating point values
        *{
            item for item in data.columns
            if "Rate" in item
        },
        # Contain non-numeric values
        *POPULATION_ADJUSTED_RATES_BROAD,
        *POPULATION_ADJUSTED_RATES_5YEAR
    }

    sample_row = produce_json(
        data.loc[
            (
                (data.areaName == 'England') &
                (data.date == sample_data.strftime("%Y-%m-%d"))
            ),
            :
        ],
        VALUE_COLUMNS,
        *non_integer
    )

    return dumps(loads(sample_row), indent=2)


def get_csv_output(data: DataFrame) -> str:
    """
    Produces a CSV file for the entire dataset for
    quality assurance purposes.
    """
    return data.to_csv(index=False, float_format="%.12g")


def calculate_pair_tested(d, pair_item, population_data):
    area_code = d.areaCode
    d = d[[*pair_item]]

    non_na = d.dropna()
    if non_na.empty or non_na.size != d.size:
        return None

    label_a, label_b = pair_item

    result = d[label_a].copy()
    item_a = sorted(d[label_a], key=lambda x: x['age'])
    item_b = sorted(d[label_b], key=lambda x: x['age'])

    for index, (d1, d2) in enumerate(zip(item_a, item_b)):
        age = d1['age']
        new_value = d1['value'] + d2['value']
        population = get_population_set(population_data, area_code, label_a)
        new_rate = new_value / population[age] * RATE_PER_POPULATION_FACTOR

        result[index] = {
            **d1,
            "value": new_value,
            "rate": round(new_rate, 1)
        }

    return result


@func_logger("total sex tested calculator")
def calculate_sex_people_tested(data, population_data, **pairs):
    data = data.assign(**{
        key: (
            data
            .loc[:, ['areaCode', 'areaType', *pair]]
            .apply(
                calculate_pair_tested,
                axis=1,
                pair_item=pair,
                population_data=population_data
            )
        )
        for key, pair in pairs.items()
    })

    return data


@func_logger("category data extractor")
def extract_category_data(data, columns, area_type, population_data):
    dt_label = DataFrame(columns=columns)

    for area_code in data[area_type]:
        area_name = data[area_type][area_code]['name']['value']
        df_code = DataFrame(columns=columns)

        for category in [*VALUE_COLUMNS, "transmissionRate"]:
            if category not in data[area_type][area_code]:
                continue

            if category == "transmissionRate":
                for item in ['min', 'max', 'growthRateMin', 'growthRateMax']:
                    df_value = DataFrame([
                        {
                            "date": row['date'],
                            "value": float(row[item]) if row[item] else row[item]
                        }
                        for row in data[area_type][area_code][category]
                    ])
                    df_value["category"] = f"transmissionRate{item[0].upper()}{item[1:]}"
                    df_value["areaCode"] = area_code
                    df_value["areaType"] = area_type
                    df_value["areaName"] = area_name

                    df_code = df_code.append(df_value)

                continue

            elif category in OUTLIERS:
                population = get_population_set(population_data, area_code, category)

                try:
                    df_value = {
                        category: process_outlier(
                            data=data[area_type][area_code][category],
                            population_set=population
                        )
                    }
                except KeyError as e:
                    logging.warning(
                        f"\t\t>> KeyError calculating rate by population for "
                        f"{(area_code, category)}: {str.join(', ', e.args)}"
                    )
                    continue

                df_value = json_normalize(df_value, [category], [])

            else:
                df_value = json_normalize(data[area_type][area_code], [category], [])
                # print(df_value.head().to_string())

            df_value["areaCode"] = area_code
            df_value["areaType"] = area_type
            df_value["areaName"] = area_name
            df_value["category"] = category

            df_code = df_code.append(df_value)

        dt_label = dt_label.append(df_code)

    return dt_label


def get_pivoted_data(data, population_data):
    columns = ["value", "date", "areaCode", "areaType", "areaName", "category"]

    dt_final = DataFrame(columns=columns)

    # Because of the hierarchical nature of the original data, there is
    # no easy way to automate this process using a generic solution
    # without prolonging the execution time. The iterative method appears
    # to produce the optimal time.
    for area_type in CATEGORY_LABELS:
        logging.info(f"\t\tArea type: {area_type}")
        dt_label = extract_category_data(data, columns, area_type, population_data)
        dt_final = dt_final.append(dt_label)

    # Reset index to appear incrementally.
    dt_final.reset_index(inplace=True)
    dt_final = dt_final.loc[:, columns]
    logging.info(">> Data was processed and converted into a categorical table")

    # Convert date strings to timestamp objects (needed for sorting).
    dt_final[DATE_COLUMN] = to_datetime(dt_final[DATE_COLUMN])
    logging.info(">> Dates were converted to datetime object")

    dt_pivot = dt_final.pivot_table(
        values='value',
        index=["areaType", "date", "areaName", "areaCode"],
        columns=['category'],
        aggfunc=lambda x: x.max()
    )

    logging.info(">> Pivot table created")

    dt_pivot.sort_values(
        ["date", "areaName"],
        ascending=[False, True],
        inplace=True
    )
    dt_pivot.reset_index(inplace=True)

    logging.info(">> Data table was sorted by date and areaName")

    # Change column names.
    dt_pivot.columns = [
        "areaType",
        "date",
        "areaName",
        "areaCode",
        *dt_pivot.columns[4:]
    ]

    logging.info(">> New column names were set")

    return dt_pivot


@func_logger("negative to zero")
def negative_to_zero(d: DataFrame):
    d.loc[:, NEGATIVE_TO_ZERO] = d[NEGATIVE_TO_ZERO].where(d[NEGATIVE_TO_ZERO] >= 0, 0)
    return d


@func_logger("cumulative calculation")
def calculate_cumulative(data: DataFrame):
    data.sort_values(
        ["areaType", "areaCode", "date"],
        ascending=[True, True, True],
        inplace=True
    )

    cumulative_names = [
        metric.replace("new", "cum")
        for metric in WITH_CUMULATIVE
    ]

    data[WITH_CUMULATIVE] = data[WITH_CUMULATIVE].astype(float)

    data[cumulative_names] = (
        data
        .loc[:, ["areaType", "areaCode", *WITH_CUMULATIVE]]
        .groupby(["areaType", "areaCode"])[WITH_CUMULATIVE]
        .cumsum()
    )

    data.sort_values(
        ["areaType", "areaName", "date"],
        ascending=[True, True, False],
        inplace=True
    )

    return data


@func_logger("main processor")
def process(data: dict, population_data: PopulationData) -> ProcessedData:
    """
    Process the data and structure them in a 2D table.

    Parameters
    ----------
    data: DataFrame
        Original data.

    population_data: PopulationData
        Population data, including all subsets of the data.

    Returns
    -------
    ProcessedData
    """
    dt_pivot = get_pivoted_data(data, population_data)

    # These must be done in a specific order.
    dt_pivot = (
        dt_pivot
        .pipe(homogenise_dates)
        .pipe(normalise_records, zero_filled=FILL_WITH_ZEROS, cumulative=START_WITH_ZERO)
        .pipe(negative_to_zero)
        .pipe(calculate_pair_summations, **DERIVED_BY_SUMMATION)
        .pipe(calculate_by_adjacent_column, **DERIVED_BY_MAX_OF_ADJACENT_COLUMN)
        .pipe(
            calculate_rates,
            population_data=population_data,
            rolling_rate=ROLLING_RATE,
            incidence_rate=INCIDENCE_RATE_FIELDS
        )  # Must be after norm
        .pipe(change_by_sum, metrics=SUM_CHANGE_DIRECTION)
        # .pipe(calculate_cumulative)
        .pipe(generate_row_hash)
        .pipe(adjust_area_types, replacements=AREA_TYPE_NAMES)
        .pipe(ratio_to_percentage, metrics=RATIO2PERCENTAGE)
        .pipe(trim_end, **TRIM_END)
        # .pipe(match_area_names)
        .pipe(
            calculate_sex_people_tested,
            population_data=population_data,
            **DERIVED_FROM_NESTED
        )
    )

    timestamp = datetime.utcnow().isoformat() + "5Z"
    dt_pivot = dt_pivot.assign(releaseTimestamp=timestamp)

    dt_pivot.sort_values(**SORT_OUTPUT_BY, inplace=True)

    result = ProcessedData(
        data_table=dt_pivot,
        csv=get_csv_output(dt_pivot),
        sample_row=get_sample_json(dt_pivot),
        timestamp=timestamp
    )

    return result


def run(new_data: str, population_data: str) -> NoReturn:
    """
    Reads the data from the blob that has been updated, then runs it
    through the processors and produces the output by setting the
    the output values.

    Parameters
    ----------
    new_data: str
        JSON data for the new file that has been uploaded.

    population_data: str
        Population reference dataset.
    """
    output_obj = MainOutput

    if DEBUG:
        output_obj = TestOutput

    pop_data = get_population_data(population_data)
    logging.info(f"\tLoaded and parsed population data")

    for key, value in RENAME.items():
        new_data = new_data.replace(key, value)

    json_data = loads(new_data)
    logging.info(f"\tParsed JSON data")

    date = datetime.now().strftime("%Y%m%d-%H%M")

    timestamp_output = output_obj(f"info/latest_available")

    archive_csv_output = output_obj(f"archive/processed_{date}.csv")
    archive_sample_row_output = output_obj(f"archive/row_sample_{date}.json")
    archive_original_data = output_obj(f"archive/processed_{date}.json")
    archive_processed_data = output_obj(f"archive/original_{date}.json")

    token_output = output_obj(f"dispatch/token.bin")
    total_records = output_obj(f"dispatch/total_records")

    try:
        result = process(json_data, pop_data)
        logging.info(f"\tFinished processing the data")

        archive_sample_row_output.set(result.sample_row)
        logging.info(f'\tStored sample row')

        if not DEBUG and ENVIRONMENT == "PRODUCTION":
            status_code = generate_report(result.csv)
            logging.info(f'\tGenerated CSV report - email request status code: {status_code}')

            # generate_og_images(result.csv)
            # logging.info(f'\tGenerated OG images')

        archive_csv_output.set(result.csv, content_type="text/csv; charset=utf-8")
        logging.info(f'\tStored CSV sample')

        timestamp_output.set(result.timestamp, content_type="text/plain; charset=utf-8")
        logging.info(f'\tStored timestamp')

        logging.info(f'> Starting to generate JSON data')

        non_integer = {
            # Contain floating point values
            *{
                item for item in result.data_table.columns
                if "Rate" in item
            },

            # Contain non-numeric values
            *POPULATION_ADJUSTED_RATES_BROAD,
            *POPULATION_ADJUSTED_RATES_5YEAR,
        }

        # dt_json = produce_json(
        #     result.data_table,
        #     (*VALUE_COLUMNS, *[item.replace("Rate", "Sum") for item in ROLLING_RATE]),
        #     *non_integer
        # )
        # logging.info(f'\tGenerated data as JSON')

        data_length = max(result.data_table.shape)
        total_records.set(str(data_length))

        archive_original_data.set(new_data)
        # archive_processed_data.set(dt_json)

    except Exception as e:
        logging.exception(e)
        raise e
    else:
        now = datetime.now()
        dir_timestamp = now.strftime('%Y-%m-%d_%H%M')
        # storage_kws = dict(
        #     compressed=False,
        #     content_type="application/json",
        #     tier="Cool"
        # )

        max_date = result.data_table.date.max()

        queue_kws = dict(
            module="db_etl",
            handler="combine_and_upload_from_file",
            container="pipeline",
            date=max_date
        )

        if DEBUG:
            dir_path = f"upload2db/{dir_timestamp}"
            makedirs(dir_path, exist_ok=True)

            # for counter, chunk in iter_chunks(loads(dt_json)):
            #     file_path = f'{dir_path}/db_{counter}.json'
            #     chunk_writer = output_obj(file_path)
            #     chunk_writer.set(chunk)

            chunk: DataFrame
            for area_type, area_code, chunk in iter_column_chunks(result.data_table):
                # file_path = f'{dir_path}/main_{area_type}_{area_code}.ft'
                save_chunk_feather(
                    data=chunk,
                    dir_path=dir_path,
                    filename=f'main_{area_code}.ft'
                )

            return None

        chunk: DataFrame
        for area_type, area_code, chunk in iter_column_chunks(result.data_table):
            file_path = f'daily_chunks/main/{max_date}'
            upload_chunk_feather(
                data=chunk,
                container="pipeline",
                dir_path=file_path,
                filename=f"{area_type}_{area_code}.ft"
            )

            enqueue_job(
                **queue_kws,
                area_type=area_type,
                area_code=area_code
            )

        # for counter, chunk in iter_chunks(loads(dt_json)):
        #     file_path = f'upload2db/{dir_timestamp}/db_{counter}.json'
        #
        #     with StorageClient("pipeline", file_path, **storage_kws) as client:
        #         client.upload(chunk)
        #
        #     enqueue_job(**queue_kws, filepath=file_path)

        logging.info(f"> Generating new dispatch token")
        token_data = generate_token()
        token_output.set(token_data, content_type="application/octet-stream")
        logging.info(f'\tNew token generated and stored')

    # if not DEBUG:
    #     # Demographic breakdowns should be queued after the main job.
    #     enqueue_job(
    #         module="demographic_etl.file_processors",
    #         handler="create_breakdown_datasets"
    #     )


def iter_column_chunks(data: DataFrame) -> Tuple[str, str, DataFrame]:
    for area_type in data.areaType.unique():
        for area_code in data.loc[data.areaType == area_type, "areaCode"].unique():
            chunk = data.loc[
                ((data.areaType == area_type) & (data.areaCode == area_code)),
                :
            ]

            yield area_type, area_code, chunk


def iter_chunks(data: List[dict]) -> Tuple[int, str]:
    chunk_size = 2000
    counter = 1

    for index in range(0, len(data), chunk_size):
        yield counter, dumps(data[index: index + chunk_size], separators=(",", ":"))
        counter += 1
        logging.info(f"\t Chunk { counter } was uploaded.")


def main(req: HttpRequest, populationData: str) -> HttpResponse:
    """
    Reads the data from the blob that has been updated, then runs it
    through the processors and produces the output by setting the
    the output values.

    See this for more -
    https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python#outputs
    https://docs.microsoft.com/en-us/python/api/azure-functions/azure.functions.out?view=azure-python

    Parameters
    ----------
    req: HttpRequest
        JSON data for the new file that has been uploaded.

    populationData: str
        Population reference dataset.
    """
    logging.info(f"--- Web hook has triggered the function. Starting the process")

    # Extract filename from the request
    request_body = req.get_body().decode()
    data_file_name = loads(request_body)["fileName"]
    logging.info(f"\tFile name loaded from the request body: {data_file_name}")

    # Confirm the file isn't archived
    seen_data = download_file("pipeline", "info/seen")

    if seen_data == data_file_name:
        return HttpResponse(status_code=HTTPStatus.OK)

    # if not DEBUG:
    #     # Demographic breakdowns should be queued after the main job.
    #     enqueue_job(
    #         module="demographic_etl.file_processors",
    #         handler="create_breakdown_datasets"
    #     )
    #
    #     enqueue_job(
    #         module="demographic_etl.file_processors",
    #         handler="create_breakdown_datasets"
    #     )

    # Download the file from storage
    logging.info(f"\tDownloading the file")
    new_data = download_file(RAW_DATA_CONTAINER, data_file_name)

    # Record in archive
    seen_output = MainOutput(f"info/seen")
    seen_output.set(data_file_name)

    run(new_data.decode(), populationData)

    return HttpResponse(status_code=HTTPStatus.OK)


if __name__ == "__main__":
    if DEBUG:
        # Local test
        from sys import stdout

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)

        with open("/Users/pouria/Downloads/data_202012141815.json") as dt_file, open("data/population.json") as pop_file:
            data_file, population_file = dt_file.read(), pop_file.read()

        run(
            new_data=data_file,
            population_data=population_file
        )
