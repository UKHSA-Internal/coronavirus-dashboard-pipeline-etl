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
from markdown import markdown

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
            "name": "New positive cases (last 7 days)",
            "label": "newCasesBySpecimenDateRollingSum",
            "metric": "newCasesBySpecimenDateRollingSum"
        },
        {
            "name": "Deaths with COVID-19 on death certificate (last 7 days)",
            "label": "newDailyNsoDeathsByDeathDateRollingSum",
            "metric": "newDailyNsoDeathsByDeathDateRollingSum"
        },
        {
            "name": "Virus tests conducted (last 7 days)",
            "label": "newVirusTestsByPublishDateRollingSum",
            "metric": "newVirusTestsByPublishDateRollingSum"
        },
        {
            "name": "Patients admitted (last 7 days)",
            "label": "hospitalCases",
            "metric": "hospitalCases"
        },
        {
            "name": "Patients in hospital",
            "label": "newAdmissions",
            "metric": "newAdmissionsRollingSum"
        },
        {
            "name": "Patients in ventilator beds",
            "label": "covidOccupiedMVBeds",
            "metric": "covidOccupiedMVBeds"
        },
        # {
        #     "name": "Total people vaccinated — autumn booster age 50+",
        #     "label": "cumPeopleVaccinatedAutumn22ByVaccinationDate50plus",
        #     "metric": "cumPeopleVaccinatedAutumn22ByVaccinationDate50plus"
        # },
        # {
        #     "name": "Vaccination uptake - autumn booster age 50+ (%)",
        #     "label": "cumVaccinationAutumn22UptakeByVaccinationDatePercentage50plus",
        #     "metric": "cumVaccinationAutumn22UptakeByVaccinationDatePercentage50plus"
        # }
    ],
]


UK_structure = [
    [
        {
            "name": "Deaths with COVID-19 on death certificate (last 7 days)",
            "label": "newDailyNsoDeathsByDeathDateRollingSum",
            "metric": "newDailyNsoDeathsByDeathDateRollingSum"
        },
        {
            "name": "Patients admitted (last 7 days)",
            "label": "newAdmissionsRollingSum",
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
    ]
]

# For tests
test_emails = {
    "to": [
        # "Recipient's email"
        "pouria.hadjibagheri@phe.gov.uk",
        "bea.goble@ukhsa.gov.uk",
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

ANNOUNCEMENTS = """\
WITH latest_release AS (
    SELECT MAX(rr.timestamp)::DATE
    FROM covid19.release_reference AS rr
    WHERE rr.released IS TRUE
)
SELECT id::TEXT,
       launch::DATE::TEXT,
       expire::DATE::TEXT,
       COALESCE(date, launch::DATE)::TEXT AS date,
       body
FROM covid19.announcement AS an
WHERE
    (
        (
                an.deploy_with_release IS TRUE
            AND an.launch::DATE <= (SELECT * FROM latest_release)
        )
      OR (
                an.deploy_with_release IS FALSE
            AND an.launch <= NOW()
        )
    )
  AND (
        (
                an.remove_with_release IS TRUE
            AND an.expire::DATE > (SELECT * FROM latest_release)
        )
      OR (
                an.remove_with_release IS FALSE
            AND an.expire > NOW()
        )
    )
ORDER BY an.launch DESC, an.expire DESC;\
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


def get_announcements():
    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(text(ANNOUNCEMENTS))
        values = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    results = list()
    for item in values:
        results.append({
            "body": markdown(item["body"].strip()),
            "date": datetime.fromisoformat(item["date"]).strftime("%-d %B %Y")
        })

    return results


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
        slug_id=get_record_id(datetime.fromisoformat(timestamp)),
        announcements=get_announcements(),
    )

    # with open("sample.html", "w") as fp:
    #     print(html_data, file=fp)

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
