#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       08 Sep 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from datetime import datetime
from json import dumps, loads
from os import getenv, path
from functools import reduce

# 3rd party:
from pandas import DataFrame
from sqlalchemy import text, select, and_, not_
from requests import post
from jinja2 import FileSystemLoader, Environment

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session, ReportRecipient
    from __app__.main_etl_postprocessors.private_report import get_record_id
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session, ReportRecipient
    from main_etl_postprocessors.private_report import get_record_id

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "2.1.2"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]

ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")
TOKEN = getenv("ReportEmailToken")
URL = getenv("ReportEmailUrl")

curr_dir = path.split(path.abspath(__file__))[0]
file_loader = FileSystemLoader(path.join(curr_dir, "templates"))
env = Environment(loader=file_loader)

England_structure = [
    [
        {
            "name": "Patients admitted",
            "label": "newAdmissions",
            "metric": "newAdmissions"
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
]


UK_structure = [
    [
        {
            "name": "New positive cases",
            "label": "newCases",
            "metric": "newCasesByPublishDate"
        },
        {
            "name": "Total positive cases",
            "label": "totalCases",
            "metric": "cumCasesByPublishDate"
        },
    ],
    [
        {
            "name": "New deaths within 28 days of a positive test",
            "label": "newDeaths",
            "metric": "newDeaths28DaysByPublishDate"
        },
        {
            "name": "Total deaths within 28 days of a positive test",
            "label": "totalDeaths",
            "metric": "cumDeaths28DaysByPublishDate"
        },
    ],
    [
        {
            "name": "Virus tests conducted",
            "label": "newVirusTestsByPublishDate",
            "metric": "newVirusTestsByPublishDate"
        },
        {
            "name": "PCR tests conducted",
            "label": "newPCRTestsByPublishDate",
            "metric": "newPCRTestsByPublishDate"
        },
        {
            "name": "Antibody tests processed",
            "label": "newAntibodyTestsByPublishDate",
            "metric": "newAntibodyTestsByPublishDate"
        }
    ],
    [
        {
            "name": "PCR testing capacity",
            "label": "plannedPCRCapacityByPublishDate",
            "metric": "plannedPCRCapacityByPublishDate"
        },
        {
            "name": "Antibody testing capacity",
            "label": "plannedAntibodyCapacityByPublishDate",
            "metric": "plannedAntibodyCapacityByPublishDate"
        },
    ],
    [
        {
            "name": "Patients admitted",
            "label": "newAdmissions",
            "metric": "newAdmissions"
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
            "name": "New people vaccinated &mdash; 1st dose",
            "label": "newPeopleVaccinatedFirstDoseByPublishDate",
            "metric": "newPeopleVaccinatedFirstDoseByPublishDate"
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
            "name": "New people vaccinated &mdash; 2nd dose",
            "label": "newPeopleVaccinatedSecondDoseByPublishDate",
            "metric": "newPeopleVaccinatedSecondDoseByPublishDate",
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
            "name": "New people vaccinated &mdash; 3rd dose and booster",
            "label": "newPeopleVaccinatedThirdInjectionByPublishDate",
            "metric": "newPeopleVaccinatedThirdInjectionByPublishDate",
        },
        {
            "name": "Total people vaccinated &mdash; 3rd dose and booster",
            "label": "cumPeopleVaccinatedThirdInjectionByPublishDate",
            "metric": "cumPeopleVaccinatedThirdInjectionByPublishDate",
        },
        {
            "name": "Vaccination uptake &mdash; 3rd dose and booster (%)",
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

# For tests
test_emails = {
    "to": [
        # "Recipient's email"
        "pouria.hadjibagheri@phe.gov.uk",
        "COVID19.Dashboard.Data@phe.gov.uk"
    ],
    "cc": list(),
    "bcc": list()
}

QUERY = """\
SELECT MAX(date)::TEXT AS date, metric, (payload -> 'value')::FLOAT AS value
FROM covid19.time_series_p{datestamp}_other AS ts
JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id 
JOIN covid19.area_reference AS ar ON ar.id = ts.area_id 
WHERE area_name = '{area_name}'
  AND metric = ANY('{metric}'::VARCHAR[])
  AND (payload ->> 'value') NOTNULL
GROUP BY metric, payload;\
"""


def extract_latest_data(date):
    output = {
        "United Kingdom": list(),
        "England": list()
    }

    metric_structs = {
        "United Kingdom": UK_structure,
        "England": England_structure
    }

    float_metric_tokens = ["rate", "uptake"]

    datestamp = datetime.fromisoformat(date)

    for struct_name, struct in metric_structs.items():
        col_names = [item["metric"] for item in reduce(lambda x, y: x + y, struct)]

        session = Session()
        conn = session.connection()
        try:
            resp = conn.execute(
                text(QUERY.format(
                    datestamp=f"{datestamp:%Y_%-m_%-d}",
                    metric=f'{{{str.join(",", col_names)}}}',
                    area_name=struct_name
                )),

            )
            values = resp.fetchall()
        except Exception as err:
            session.rollback()
            raise err
        finally:
            session.close()

        data = (
            DataFrame(values, columns=["date", "metric", "value"])
            .pivot_table(
                values="value",
                index=["date"],
                columns=["metric"],
                aggfunc="first"
            )
            .reset_index()
        )

        for collection in struct:
            coll = list()
            for item in collection:
                label, metric, name = item['label'], item['metric'], item["name"]

                try:
                    date = (
                        data
                        .loc[:, ["date", metric]]
                        .dropna()
                        .date
                        .max()
                    )

                    value = data.loc[data.date == date, metric].values.tolist().pop()

                    if any(token in metric.lower() for token in float_metric_tokens):
                        value_fmt = format(value, ".1f")
                    else:
                        value_fmt = format(int(value), ",d")

                    coll.append({
                        "name": name,
                        "date": datetime.strptime(date, "%Y-%m-%d").strftime("%d %b %Y"),
                        "value": value_fmt
                    })

                except Exception as err:
                    logging.error(f"Failed to extract data for the daily - {err}")

                    coll.append({
                        f"name": name,
                        f"date": "N/A",
                        f"value": "N/A"
                    })
            output[struct_name].append(coll)

    return output


def get_email_recipients():
    session = Session()

    query = session.execute(
        select([
            ReportRecipient.recipient
        ])
        .where(
            and_(
                ReportRecipient.deactivated == False,
                not_(ReportRecipient.approved_by == None)
            )
        )
    )

    return [item[0] for item in query]


def main(payload):
    logging.info(
        f"Daily report triggered in environment '{ENVIRONMENT}' with "
        f"payload: {payload}"
    )

    if payload['environment'] != "PRODUCTION" or payload['legacy']:
        return payload

    timestamp = payload['timestamp']

    data = extract_latest_data(timestamp)

    logging.info(f"\tRendering the template")
    template = env.get_template("base.html")

    html_data = template.render(
        results=data,
        timestamp=datetime.now().strftime(r"%A, %d %b %Y at %H:%M:%S GMT"),
        slug_id=get_record_id(datetime.fromisoformat(timestamp))
    )

    # with open("sample.html", "w") as fp:
    #     print(html_data, file=fp)

    logging.info(f"\tLoading email recipients' data.")
    with StorageClient(container="pipeline", path="assets/email_recipients.json") as client:
        email_data = loads(client.download().readall().decode())

    logging.info(f"\tSubmitting the request to send out the email.")
    response = post(
        url=URL,
        data=dumps({
            "body": html_data,
            "to": "COVID19.Dashboard.Data@phe.gov.uk",
            "bcc": str.join("; ", get_email_recipients())
        }),
        headers={"token": TOKEN, "Content-Type": "application/json"}
    )

    return str(response.status_code)


def _main_test(payload):
    from pprint import pprint

    if ENVIRONMENT != "PRODUCTION":
        return f"NOT SENT (env={ENVIRONMENT})"

    timestamp = payload['timestamp']
    # with StorageClient(container="pipeline", path=data_path) as client:
    #     feather_io = BytesIO(client.download().readall())

    data = extract_latest_data(timestamp)

    pprint(data)


if __name__ == "__main__":
    from datetime import timedelta
    main({
        "timestamp": (datetime.now() - timedelta(days=0)).isoformat(),
        "environment": "PRODUCTION",
        "legacy": False
    })
    # pass
    # from pandas import read_feather
    # from io import BytesIO
    #
    # with StorageClient("pipeline", "etl/processed/2021-03-09_1456/overview_K02000001.ft") as cli:
    #     data_io = BytesIO(cli.download().readall())
    #     data_io.seek(0)
    #
    #
    # # print(read_feather(data_io))
    # # with open("/Users/pouria/Downloads/processed_2021-01-21.csv", "r") as output_sample:
    # print(main_test(data_io))
