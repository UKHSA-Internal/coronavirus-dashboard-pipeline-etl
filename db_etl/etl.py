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
from io import BytesIO
from tempfile import TemporaryFile
from pathlib import Path

# 3rd party:
from azure.storage.blob import BlobClient, BlobType, ContentSettings, StandardBlobTier

from pandas import (
    DataFrame, to_datetime, json_normalize,
    read_feather, read_csv
)

# Internal
try:
    from __app__.utilities import (
        func_logger, get_population_data
    )
    from __app__.utilities.generic_types import RawDataPayload, PopulationData
    from .output import produce_json
    from __app__.storage import StorageClient
    from .processors import (
        homogenise_dates, normalise_records,
        calculate_pair_summations, calculate_by_adjacent_column,
        calculate_rates, generate_row_hash, change_by_sum,
        ratio_to_percentage, trim_end, match_area_names,
        normalise_demographics_records, homogenise_demographics_dates,
        calculate_age_rates
    )
    from .db_uploader.chunk_ops import (
        save_chunk_feather, upload_chunk_feather
    )
except ImportError:
    from utilities import (
        func_logger, get_population_data
    )
    from utilities.generic_types import RawDataPayload, PopulationData
    from storage import StorageClient
    from db_etl.output import produce_json
    from db_etl.processors import (
        homogenise_dates, normalise_records,
        calculate_pair_summations, calculate_by_adjacent_column,
        calculate_rates, generate_row_hash, change_by_sum,
        ratio_to_percentage, trim_end, match_area_names,
        normalise_demographics_records, homogenise_demographics_dates,
        calculate_age_rates
    )
    from db_etl.db_uploader.chunk_ops import (
        save_chunk_feather, upload_chunk_feather
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'run',
    'run_direct',
    'run_demographics',
    'run_direct_msoas'
]


ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")

CURRENT_PATH = Path(__file__).parent.resolve()

# DEBUG = True and not ENVIRONMENT == "PRODUCTION"
# DEBUG = False

DEBUG = getenv("DEBUG", False)

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

    # "femaleNegatives",  # Deprecated
    # "previouslyReportedCumCasesBySpecimenDate",  # Deprecated
    # "changeInCumCasesBySpecimenDate",  # Deprecated
    # "cumNegativesBySpecimenDate",  # Deprecated
    # "newNegativesBySpecimenDate",  # Deprecated
    # "maleNegatives",  # Deprecated

    "maleCases",
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
    "newCasesByPublishDate",
    "femaleCases",
    "cumAdmissionsByAge",
    "newPillarThreeTestsByPublishDate",
    "cumPillarFourTestsByPublishDate",
    "newTestsByPublishDate",
    "newPillarTwoTestsByPublishDate",
    "cumPeopleTestedByPublishDate",
    "cumPillarOneTestsByPublishDate",
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
    'newVirusTestsByPublishDate',
    'cumVirusTestsByPublishDate',

    "newOnsCareHomeDeathsByRegistrationDate",
    "cumOnsCareHomeDeathsByRegistrationDate",

    "uniqueCasePositivityBySpecimenDateRollingSum",
    "uniquePeopleTestedBySpecimenDateRollingSum",

    'newPeopleReceivingFirstDose',
    'cumPeopleReceivingFirstDose',
    'newPeopleReceivingSecondDose',
    'cumPeopleReceivingSecondDose',

    'cumWeeklyNsoDeathsByRegDate',
    'newWeeklyNsoDeathsByRegDate',
    'cumWeeklyNsoCareHomeDeathsByRegDate',
    'newWeeklyNsoCareHomeDeathsByRegDate',

    'newDailyNsoDeathsByDeathDate',
    'cumDailyNsoDeathsByDeathDate',

    "newPeopleVaccinatedFirstDoseByPublishDate",
    "cumPeopleVaccinatedFirstDoseByPublishDate",

    "newPeopleVaccinatedSecondDoseByPublishDate",
    "cumPeopleVaccinatedSecondDoseByPublishDate",

    "newPeopleVaccinatedCompleteByPublishDate",
    "cumPeopleVaccinatedCompleteByPublishDate",

    "weeklyPeopleVaccinatedFirstDoseByVaccinationDate",  # Deprecated
    "weeklyPeopleVaccinatedSecondDoseByVaccinationDate",  # Deprecated
    "cumPeopleVaccinatedFirstDoseByVaccinationDate",
    "cumPeopleVaccinatedSecondDoseByVaccinationDate",

    "newCasesPCROnlyBySpecimenDate",
    "cumCasesPCROnlyBySpecimenDate",
    "newCasesLFDOnlyBySpecimenDate",
    "cumCasesLFDOnlyBySpecimenDate",
    "newCasesLFDConfirmedPCRBySpecimenDate",
    "cumCasesLFDConfirmedPCRBySpecimenDate",

    "newVaccinesGivenByPublishDate",
    "cumVaccinesGivenByPublishDate",

    "cumVaccinationFirstDoseUptakeByPublishDatePercentage",
    "cumVaccinationSecondDoseUptakeByPublishDatePercentage",
    "cumVaccinationCompleteCoverageByPublishDatePercentage",

    "cumPeopleVaccinatedThirdInjectionByVaccinationDate",
    "newPeopleVaccinatedThirdInjectionByVaccinationDate",
    "cumVaccinationThirdInjectionUptakeByVaccinationDatePercentage",
)


WITH_CUMULATIVE = []


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
    "nation",
    "region",
    "nhsRegion",
    "nhsTrust",
    "utla",
    "ltla"
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

    "newPeopleVaccinatedFirstDoseByPublishDate",
    "newPeopleVaccinatedSecondDoseByPublishDate",
    "newPeopleVaccinatedCompleteByPublishDate",
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
    # 'maleNegatives',   # Deprecated
    # 'femaleNegatives',  # Deprecated
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

    "cumWeeklyNsoDeathsByRegDate",
}


ROLLING_RATE = {
    "newCasesBySpecimenDate",
    "newCasesByPublishDate",
    "newDeathsByDeathDate",
    "newDeaths28DaysByDeathDate",
    "newDeaths60DaysByDeathDate",
    "newAdmissions",
    'newCasesPCROnlyBySpecimenDate',
    'newCasesLFDOnlyBySpecimenDate',
    'newCasesLFDConfirmedPCRBySpecimenDate',
}


# These get incidence rate calculated.
DERIVED_FROM_NESTED = {
    # "femalePeopleTested": ("femaleCases", "femaleNegatives"),
    # "malePeopleTested": ("maleCases", "maleNegatives")
}


DERIVED_BY_SUMMATION = {
    # "cumPeopleTestedBySpecimenDate":
    #     ("cumCasesBySpecimenDate", "cumNegativesBySpecimenDate"),  # Deprecated
    #
    # "newPeopleTestedBySpecimenDate":
    #     ("newCasesBySpecimenDate", "newNegativesBySpecimenDate")  # Deprecated
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
    # 'newNegativesBySpecimenDate',  # Deprecated
    'newCasesBySpecimenDate',
    'newLFDTests',
    'newDailyNsoDeathsByDeathDate',
    'newCasesPCROnlyBySpecimenDate',
    'newCasesLFDOnlyBySpecimenDate',
    'newCasesLFDConfirmedPCRBySpecimenDate',

    # Vaccine demogs
    'newPeopleVaccinatedFirstDoseByVaccinationDate',
    'newPeopleVaccinatedSecondDoseByVaccinationDate',
    'newPeopleVaccinatedCompleteByVaccinationDate',
    'newPeopleVaccinatedThirdInjectionByVaccinationDate',

    'newVirusTestsBySpecimenDate',
    'newPCRTestsBySpecimenDate',
}


START_WITH_ZERO = {
    'cumDeathsByPublishDate',
    'cumDeathsByDeathDate',
    'cumCasesBySpecimenDate',
    'cumNegativesBySpecimenDate',
    'cumLFDTests',
    'cumDeaths28DaysByDeathDate',
    'cumDeaths60DaysByDeathDate',
    'cumDailyNsoDeathsByDeathDate',
    'cumCasesPCROnlyBySpecimenDate',
    'cumCasesLFDOnlyBySpecimenDate',
    'cumCasesLFDConfirmedPCRBySpecimenDate',

    # Vaccine demogs
    'cumPeopleVaccinatedFirstDoseByVaccinationDate',
    'cumPeopleVaccinatedSecondDoseByVaccinationDate',
    'cumPeopleVaccinatedThirdInjectionByVaccinationDate',
    'cumPeopleVaccinatedCompleteByVaccinationDate',
    'cumVaccinationFirstDoseUptakeByVaccinationDatePercentage',
    'cumVaccinationSecondDoseUptakeByVaccinationDatePercentage',
    'cumVaccinationThirdInjectionUptakeByVaccinationDatePercentage',
    'cumVaccinationCompleteCoverageByVaccinationDatePercentage',
    'VaccineRegisterPopulationByVaccinationDate',

    'cumVirusTestsBySpecimenDate',
    'cumPCRTestsBySpecimenDate',
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
    'newVirusTestsByPublishDate'
}


# Get incidence rate calculated.
OUTLIERS = [
    'maleCases',
    'femaleCases',
    # 'maleNegatives',  # Deprecated
    # 'femaleNegatives',  # Deprecated
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
    for area_type in data:
        if area_type not in CATEGORY_LABELS:
            continue

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
    cols = set(NEGATIVE_TO_ZERO).intersection(d.columns)

    if not cols:
        return d

    for col in cols:
        d.loc[d[col] < 0, col] = 0

    return d


@func_logger("cumulative calculation")
def calculate_cumulative(data: DataFrame, metrics):
    data.sort_values(
        ["areaType", "areaCode", "date"],
        ascending=[True, True, True],
        inplace=True
    )

    cumulative_names = [
        metric.replace("new", "cum")
        for metric in metrics
    ]

    data[metrics] = data[metrics].astype(float)

    data[cumulative_names] = (
        data
        .loc[:, ["areaType", "areaCode", *metrics]]
        .groupby(["areaType", "areaCode"])[metrics]
        .cumsum()
    )

    data.sort_values(
        ["areaType", "areaName", "date"],
        ascending=[True, True, False],
        inplace=True
    )

    return data


@func_logger("main processor")
def process(data: DataFrame,
            population_data: PopulationData,
            payload: RawDataPayload, is_direct=False) -> DataFrame:
    """
    Process the data and structure them in a 2D table.

    Parameters
    ----------
    data: dict
        Original data.

    population_data: PopulationData

    payload: RawDataPayload

    is_direct: bool

    Returns
    -------
    DataFrame
    """
    if not is_direct:
        dt_pivot = get_pivoted_data(data, population_data)
    else:
        dt_pivot = data

    columns = set(dt_pivot.columns)

    release_date = payload.timestamp.split('T')[0]

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
        # .pipe(calculate_cumulative, WITH_CUMULATIVE)
        .pipe(generate_row_hash, date=release_date)
        # .pipe(adjust_area_types, replacements=AREA_TYPE_NAMES)
        .pipe(ratio_to_percentage, metrics=RATIO2PERCENTAGE)
        .pipe(trim_end, **TRIM_END)
        # .pipe(match_area_names)
        # .pipe(
        #     calculate_sex_people_tested,
        #     population_data=population_data,
        #     **DERIVED_FROM_NESTED
        # )
        .assign(releaseTimestamp=payload.timestamp)
        .sort_values(**SORT_OUTPUT_BY)
    )

    return dt_pivot


def run_direct(payload_dict: dict):
    logging.info(f"run_direct:: {payload_dict}")
    payload = RawDataPayload(**payload_dict["base"])
    category = payload_dict['category']
    subcategory = payload_dict.get('subcategory')
    area_type = payload_dict['area_type']
    area_code = payload_dict['area_code']
    date = payload_dict['date']

    if category == "vaccination" and subcategory == "age_demographics":
        return run_demographics(payload_dict)

    kws = dict(
        container="pipeline",
        content_type="application/octet-stream",
        cache_control="no-cache, max-age=0, must-revalidate",
        compressed=False,
        tier='Cool'
    )

    # Retrieve data chunk
    with StorageClient(**kws, path=payload.data_path) as client, TemporaryFile() as fp:
        if not client.exists():
            raise RuntimeError(f"Blob not found: {payload.data_path}")

        client.download().readinto(fp)
        fp.seek(0)
        data = read_feather(fp)

    # Demographics
    population_data = get_population_data()
    logging.info(f"\tLoaded and parsed population data")

    # Process chunk
    result = process(data, population_data, payload, is_direct=True)

    # Store chunk for deployment to DB
    result_path = f"daily_chunks/{category}/{date}/{area_type}_{area_code}.ft"
    with TemporaryFile() as fp:
        result.reset_index(drop=True).to_feather(fp)
        fp.seek(0)

        with StorageClient(**kws, path=result_path) as cli:
            cli.upload(fp.read())

    response_payload = {
        "path": result_path,
        "area_code": area_code,
        "area_type": area_type,
        "date": date,
        "environment": payload.environment,
        "category": category,
        "subcategory": subcategory
    }

    logging.info(response_payload)

    return response_payload


def run_direct_msoas(payload_dict: dict):
    logging.info(f"run_direct:: {payload_dict}")
    payload = RawDataPayload(**payload_dict["base"])
    category = payload_dict['category']
    subcategory = payload_dict.get('subcategory')
    area_type = payload_dict['area_type']
    area_code = payload_dict['area_code']
    date = payload_dict['date']

    if category == "vaccination" and subcategory == "age_demographics":
        return run_demographics(payload_dict)

    kws = dict(
        container="pipeline",
        content_type="application/octet-stream",
        cache_control="no-cache, max-age=0, must-revalidate",
        compressed=False,
        tier='Cool'
    )

    # Retrieve data chunk
    with StorageClient(**kws, path=payload.data_path) as client, TemporaryFile() as fp:
        if not client.exists():
            raise RuntimeError(f"Blob not found: {payload.data_path}")

        client.download().readinto(fp)
        fp.seek(0)
        data = read_feather(fp)

    # Process chunk
    # These must be done in a specific order.
    result = (
        data
        .pipe(homogenise_dates)
        .pipe(normalise_records, zero_filled=FILL_WITH_ZEROS, cumulative=START_WITH_ZERO)
    )

    # Store chunk for deployment to DB
    result_path = f"daily_chunks/{category}/{date}/{area_type}_{area_code}.ft"
    with TemporaryFile() as fp:
        result.reset_index(drop=True).to_feather(fp)
        fp.seek(0)

        with StorageClient(**kws, path=result_path) as cli:
            cli.upload(fp.read())

    response_payload = {
        "path": result_path,
        "area_code": area_code,
        "area_type": area_type,
        "date": date,
        "environment": payload.environment,
        "category": category,
        "subcategory": subcategory
    }

    logging.info(response_payload)

    return response_payload


def get_prepped_age_breakdown_population():
    path = CURRENT_PATH.joinpath("assets", "prepped_demographics_population.csv").resolve()

    return read_csv(path, index_col=["areaCode", "age"])


def metric_specific_processes(df, base_metric, db_payload_metric):
    if base_metric is None:
        return df
    else:
        df = (
            df
            .pipe(
                calculate_age_rates,
                population_data=get_prepped_age_breakdown_population(),
                max_date=df.date.max(),
                rolling_rate=[base_metric]
            )
        )

        new_names = {
            col: col.replace(base_metric, "")
            for col in df.columns if col.startswith(base_metric) and base_metric != col
        }

        new_names = {
            col: new_col[0].lower() + new_col[1:]
            for col, new_col in new_names.items()
        }

        cutoff_date = f"{datetime.now() - timedelta(days=5):%Y-%m-%d}"

        df = (
            df
            .rename(columns={**new_names, base_metric: db_payload_metric})
            .loc[df.date <= cutoff_date, :]  # Drop the last 5 days (event date data)
        )

        # Convert non-decimal columns to integer type
        # to prevent `.0` in JSON payloads.
        df.loc[:, [db_payload_metric, "rollingSum"]] = (
            df
            .loc[:, [db_payload_metric, "rollingSum"]]
            .astype("Int64")
        )

        return df


def run_demographics(payload_dict):
    logging.info(f"run_demographics:: {payload_dict}")

    metric_names_ref = {
        "vaccinations-by-vaccination-date": {
            "age-demographics": {
                "metric_name": "vaccinationsAgeDemographics",
                "main_metrics": ['areaType', 'areaCode', 'areaName', 'date', 'age']
            }
        },
        "cases-by-specimen-date": {
            "age-demographics": {
                "metric_name": "newCasesBySpecimenDateAgeDemographics",
                "base_metric": "newCasesBySpecimenDate",
                "db_payload_metric": "cases",
                "main_metrics": ['areaType', 'areaCode', 'areaName', 'date', 'age']
            }
        },
        "deaths28days-by-death-date": {
            "age-demographics": {
                "metric_name": "newDeaths28DaysByDeathDateAgeDemographics",
                "base_metric": "newDeaths28DaysByDeathDate",
                "db_payload_metric": "deaths",
                "main_metrics": ['areaType', 'areaCode', 'areaName', 'date', 'age']
            }
        },
    }

    payload = RawDataPayload(**payload_dict["base"])
    category = payload_dict['category']
    subcategory = payload_dict['subcategory']
    area_type = payload_dict['area_type']
    area_code = payload_dict['area_code']
    date = payload_dict['date']

    metadata = metric_names_ref[category][subcategory]
    metric_name = metadata["metric_name"]

    kws = dict(
        container="pipeline",
        content_type="application/octet-stream",
        cache_control="no-cache, max-age=0, must-revalidate",
        compressed=False,
        tier='Cool'
    )

    # Retrieve data chunk
    with StorageClient(**kws, path=payload.data_path) as client, TemporaryFile() as fp:
        if not client.exists():
            raise RuntimeError(f"Blob not found: {payload.data_path}")

        client.download().readinto(fp)
        fp.seek(0)
        data = read_feather(fp)

    logging.info(f"\tLoaded and parsed population data")

    main_metrics = metadata["main_metrics"]
    metrics = data.columns[~data.columns.isin(main_metrics)]

    db_payload_metric = metadata.get("db_payload_metric")
    if db_payload_metric is not None:
        metrics = [db_payload_metric, "rollingSum", "rollingRate"]
        logging.info(metrics)

    result = (
        data
        .pipe(homogenise_demographics_dates)
        .set_index(main_metrics)
        .pipe(
            normalise_demographics_records,
            zero_filled=FILL_WITH_ZEROS,
            cumulative=START_WITH_ZERO
        )
        .pipe(
            metric_specific_processes,
            base_metric=metadata.get("base_metric"),
            db_payload_metric=db_payload_metric
        )
        .groupby(main_metrics[:-1])
        .apply(lambda x: x.loc[:, [main_metrics[-1], *metrics]].to_dict(orient="records"))
        .reset_index()
        .rename(columns={0: metric_name})
    )

    # Store chunk for deployment to DB
    result_path = f"daily_chunks/{category}/{subcategory}/{date}/{area_type}_{area_code}.ft"
    with TemporaryFile() as fp:
        result.reset_index(drop=True).to_feather(fp)
        fp.seek(0)

        with StorageClient(**kws, path=result_path) as cli:
            cli.upload(fp.read())

    response_payload = {
        "path": result_path,
        "area_code": area_code,
        "area_type": area_type,
        "date": date,
        "environment": payload.environment,
        "category": category,
        "subcategory": subcategory
    }

    return response_payload


def run(payload_dict: dict):
    """
    Reads the data from the blob that has been updated, then runs it
    through the processors and produces the output by setting the
    the output values.

    Parameters
    ----------
    payload_dict: RawDataPayload
        Data payload.
    """
    payload = RawDataPayload(**payload_dict)
    logging.info(f"Starting ETL process with payload: {payload_dict}")

    raw_data_chunk_kws = dict(
        container="pipeline",
        content_type="application/json; charset=utf-8",
        cache_control="no-cache, max-age=0",
        compressed=False,
        tier='Cool'
    )

    # Demographics
    population_data = get_population_data()
    logging.info(f"\tLoaded and parsed population data")

    try:
        with StorageClient(**raw_data_chunk_kws, path=payload.data_path) as client:
            if not client.exists():
                raise RuntimeError(f"Blob not found: {payload.data_path}")
            data = loads(client.download().readall().decode())

        area_type: str = list(data.keys())[0]
        # area_type = area_type.rstrip('s')
        area_code: str = list(data[area_type].keys())[0]

        result = process(data, population_data, payload)
        logging.info(f"\tFinished ETL process for payload: {payload_dict}")
    except Exception as e:
        logging.critical(f"FAILED: {payload_dict}")
        logging.exception(e)
        raise e
    else:
        timestamp = datetime.fromisoformat(payload.timestamp[:26])
        dir_timestamp = f"{timestamp:%Y-%m-%d_%H%M}"
        max_date = f"{timestamp:%Y-%m-%d}"

        if DEBUG:
            dir_path = f"upload2db/{dir_timestamp}"
            makedirs(dir_path, exist_ok=True)

            # chunk: DataFrame
            # for area_type, area_code, chunk in iter_column_chunks(result):
            #     save_chunk_feather(
            #         data=chunk,
            #         dir_path=dir_path,
            #         filename=f'main_{area_code}.ft'
            #     )

            save_chunk_feather(
                data=result,
                dir_path=dir_path,
                filename=f'main_{area_code}.ft'
            )

        else:
            chunk: DataFrame
            # for area_type, area_code, chunk in iter_column_chunks(result):
            file_path = f'daily_chunks/main/{max_date}'
            upload_chunk_feather(
                data=result,
                container="pipeline",
                dir_path=file_path,
                filename=f"{area_type}_{area_code}.ft"
            )

            processed_data_kws = dict(
                container="pipeline",
                content_type="application/octet-stream",
                compressed=False,
                cache_control="no-cache, max-age=0",
                tier='Cool'
            )

            path = f"etl/processed/{dir_timestamp}/{area_type}_{area_code}.ft"

            file_obj = BytesIO()

            try:
                result.reset_index(drop=True).to_feather(file_obj)
            except Exception as e:
                logging.critical(f"FAILED ON {path}")
                logging.exception(e)
                raise e

            file_obj.seek(0)
            bin_data = file_obj.read()

            with StorageClient(**processed_data_kws, path=path) as client:
                client.upload(bin_data)

            return {
                "path": path,
                "area_code": area_code,
                "area_type": area_type,
                "date": max_date,
                "environment": payload.environment
            }


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

