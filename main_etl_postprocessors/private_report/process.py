#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv, path
from hashlib import blake2b
from datetime import datetime

# 3rd party:
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from jinja2 import FileSystemLoader, Environment

# Internal: 
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session, PrivateReport
    from __app__.main_etl_postprocessors.private_report.queries import MAIN_QUERY, OUTPUT_DATA
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session, PrivateReport
    from main_etl_postprocessors.private_report.queries import MAIN_QUERY, OUTPUT_DATA

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "process",
    "get_record_id"
]

RECORD_KEY = getenv("RECORD_KEY").encode()

curr_dir = path.split(path.abspath(__file__))[0]
file_loader = FileSystemLoader(path.join(curr_dir, "templates"))
env = Environment(loader=file_loader)

METRICS = [
    'newAdmissionsRollingSum',
    'newAdmissionsChange',
    'newAdmissionsChangePercentage',
    'hospitalCases',
    'covidOccupiedMVBeds',
    'newCasesByPublishDateRollingSum',
    'newCasesByPublishDateChange',
    'newCasesByPublishDateChangePercentage',
    'cumCasesByPublishDateRollingSum',
    'newDeaths28DaysByPublishDate',
    'cumDeaths28DaysByPublishDate',
    'newVirusTestsByPublishDateRollingSum',
    'newVirusTestsByPublishDateChange',
    'newVirusTestsByPublishDateChangePercentage',
    'uniqueCasePositivityBySpecimenDateRollingSum',
    'newPCRTestsByPublishDate',
    'newAntibodyTestsByPublishDate',
    'plannedPCRCapacityByPublishDate',
    'plannedAntibodyCapacityByPublishDate',
    'newAdmissions',
    'hospitalCases',
    'covidOccupiedMVBeds',
    'newPeopleVaccinatedFirstDoseByPublishDateRollingSum',
    'cumPeopleVaccinatedFirstDoseByPublishDate',
    'cumVaccinationFirstDoseUptakeByPublishDatePercentage',
    'newPeopleVaccinatedSecondDoseByPublishDateRollingSum',
    'cumPeopleVaccinatedSecondDoseByPublishDate',
    'cumVaccinationSecondDoseUptakeByPublishDatePercentage',
    'newPeopleVaccinatedThirdInjectionByPublishDateRollingSum',
    'cumPeopleVaccinatedThirdInjectionByPublishDate',
    'cumVaccinationThirdInjectionUptakeByPublishDatePercentage',
    'newVaccinesGivenByPublishDate',
    'cumVaccinesGivenByPublishDate'
]

structure = [
    [
        {
            "name": "New positive cases (last 7 days)",
            "label": "newCases",
            "metric": "newCasesByPublishDateRollingSum",
        },
        {
            "name": "Total positive cases",
            "label": "totalCases",
            "metric": "cumCasesByPublishDate"
        },
    ],
    [
        {
            "name": "New deaths within 28 days of a positive test (last 7 days)",
            "label": "newDeaths",
            "metric": "newDeaths28DaysByPublishDateRollingSum"
        },
        {
            "name": "Total deaths within 28 days of a positive test",
            "label": "totalDeaths",
            "metric": "cumDeaths28DaysByPublishDate"
        },
    ],
    [
        {
            "name": "Virus tests conducted (last 7 days)",
            "label": "newVirusTestsByPublishDate",
            "metric": "newVirusTestsByPublishDateRollingSum"
        },
        {
            "name": "Positivity ratio (%)",
            "label": "uniqueCasePositivityBySpecimenDateRollingSum",
            "metric": "uniqueCasePositivityBySpecimenDateRollingSum"
        },
    ],
    [
        {
            "name": "Patients admitted (last 7 days)",
            "label": "newAdmissions",
            "metric": "newAdmissionsRollingSum"
        },
        {
            "name": "Patients in hospital",
            "label": "hospitalCases",
            "metric": "hospitalCases"
        },
        {
            "name": "Patients in ventilator beds",
            "label": "covidOccupiedMVBeds",
            "metric": "covidOccupiedMVBeds"
        }
    ],
    [
        {
            "name": "New people vaccinated &mdash; 1st dose (last 7 days)",
            "label": "newPeopleVaccinatedFirstDoseByPublishDateRollingSum",
            "metric": "newPeopleVaccinatedFirstDoseByPublishDateRollingSum"
        },
        {
            "name": "Total people vaccinated &mdash; 1st dose",
            "label": "cumPeopleVaccinatedFirstDoseByPublishDate",
            "metric": "cumPeopleVaccinatedFirstDoseByPublishDate"
        },
        {
            "name": "Vaccination uptake &mdash; 1st dose (%)",
            "label": "cumVaccinationFirstDoseUptakeByPublishDatePercentage",
            "metric": "cumVaccinationFirstDoseUptakeByPublishDatePercentage"
        },
    ],
    [
        {
            "name": "New people vaccinated &mdash; 2nd dose (last 7 days)",
            "label": "newPeopleVaccinatedSecondDoseByPublishDateRollingSum",
            "metric": "newPeopleVaccinatedSecondDoseByPublishDateRollingSum",
        },
        {
            "name": "Total people vaccinated &mdash; 2nd dose",
            "label": "cumPeopleVaccinatedSecondDoseByPublishDate",
            "metric": "cumPeopleVaccinatedSecondDoseByPublishDate",
        },
        {
            "name": "Vaccination uptake &mdash; 2nd dose (%)",
            "label": "cumVaccinationSecondDoseUptakeByPublishDatePercentage",
            "metric": "cumVaccinationSecondDoseUptakeByPublishDatePercentage"
        },
    ],
    [
        {
            "name": "New people vaccinated &mdash; 3rd dose + booster (last 7 days)",
            "label": "newPeopleVaccinatedThirdInjectionByPublishDateRollingSum",
            "metric": "newPeopleVaccinatedThirdInjectionByPublishDateRollingSum",
        },
        {
            "name": "Total people vaccinated &mdash; 3rd dose + booster",
            "label": "cumPeopleVaccinatedThirdInjectionByPublishDate",
            "metric": "cumPeopleVaccinatedThirdInjectionByPublishDate",
        },
        {
            "name": "Vaccination uptake &mdash; 3rd dose + booster (%)",
            "label": "cumVaccinationThirdInjectionUptakeByPublishDatePercentage",
            "metric": "cumVaccinationThirdInjectionUptakeByPublishDatePercentage"
        },
    ],
    [
        {
            "name": "Daily vaccines given",
            "label": "newVaccinesGivenByPublishDate",
            "metric": "newVaccinesGivenByPublishDate",
        },
        {
            "name": "Total vaccines given",
            "label": "cumVaccinesGivenByPublishDate",
            "metric": "cumVaccinesGivenByPublishDate",
        }
    ]
]


def get_record_id(record_date: datetime):
    date = f"{record_date:%Y%m%d}".encode()
    hashed = blake2b(date, digest_size=20, key=RECORD_KEY)
    return hashed.hexdigest()


def store_data(data: DataFrame):
    if not data.size:
        return None

    session = Session()
    connection = session.connection()
    try:
        records = data.to_dict(orient="records")

        insert_stmt = insert(PrivateReport.__table__).values(records)
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[
                PrivateReport.slug_id,
                PrivateReport.date,
                PrivateReport.metric,
                PrivateReport.area_id
            ],
            set_={PrivateReport.value.name: insert_stmt.excluded.value}
        )
        connection.execute(stmt)
        session.flush()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()


def get_data(record_date: datetime) -> DataFrame:
    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(MAIN_QUERY.format(partition_date=f"{record_date:%Y_%-m_%-d}")),
            metrics=METRICS
        )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    dt = (
        DataFrame(raw_data, columns=["release_id", "area_id", "metric", "date", "value"])
        .assign(slug_id=get_record_id(record_date))
    )

    return dt


def format_number(num):
    if int(num) != num:
        return format(num, ".1f")
    else:
        return format(int(num), ",d")


def store_html(data, slug_id):
    kws = dict(
        container="ondemand",
        path=f"prerelease/{slug_id}.html",
        content_type="text/html; charset=UTF-8",
        cache_control="public, max-age=120, must-revalidate"
    )

    with StorageClient(**kws) as cli:
        cli.upload(data)


def process_section(df, section, area_names):
    result_items = dict()
    for area_name in area_names:
        payload = df.loc[df.area_name == area_name, :].copy()

        try:
            section["date"] = (
                payload
                .loc[df.metric == section['metric'], 'date']
                .values[0]
                .strftime("%d %b %Y")
            )
            section["value"] = (
                payload
                .loc[df.metric == section['metric'], 'value']
                .values[0]
            )
        except IndexError:
            section["date"] = "&mdash;"
            section["value"] = "&mdash;"

        try:
            section["change"] = (
                payload
                .loc[df.metric == f"{section['metric']}Change", "value"]
                .values[0]
            )
            section["percentage_change"] = (
                payload
                .loc[df.metric == f"{section['metric']}ChangePercentage", "value"]
                .values[0]
            )
        except IndexError:
            section["change"] = "&mdash;"
            section["percentage_change"] = "&mdash;"

        result_items[area_name] = section.copy()

    return result_items


def generate_html(record_date: datetime):
    slug_id = get_record_id(record_date)

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(text(OUTPUT_DATA), slug_id=slug_id)
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    df = DataFrame(raw_data, columns=["area_name", "metric", "date", "value"])
    df.value = df.value.map(format_number)

    area_names = [
        "United Kingdom",
        "England",
        "Northern Ireland",
        "Scotland",
        "Wales"
    ]

    result = list()

    for section in structure:
        result.append([
            process_section(df, item, area_names)
            for item in section
        ])

    template = env.get_template("base.html")

    html_data = template.render(
        results={
            "area_names": area_names,
            "data": result
        },
        timestamp=datetime.now().strftime(r"%A, %d %b %Y at %H:%M:%S GMT")
    )

    # Uncomment for testing
    # with open("sample.html", "w") as fp:
    #     print(html_data, file=fp)

    # Comment for testing
    store_html(html_data, slug_id=slug_id)


def process(payload):
    date = datetime.fromisoformat(payload['timestamp'])
    data = get_data(date)
    store_data(data)
    generate_html(date)

    return f"DONE: {payload['timestamp']}"


# Manual run
if __name__ == "__main__":
    from datetime import timedelta

    delta = 0  # days
    process({
        "timestamp": (datetime.now() - timedelta(days=delta)).isoformat()
    })
