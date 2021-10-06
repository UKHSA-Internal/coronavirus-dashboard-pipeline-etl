#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       22 Jun 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os.path import abspath, split, join
from os import getenv
from datetime import datetime
from http import HTTPStatus
from gzip import compress

# 3rd party:
from jinja2 import FileSystemLoader, Environment
from azure.functions import HttpRequest, HttpResponse
from azure.storage.queue import QueueClient, BinaryBase64EncodePolicy

# Internal:
try:
    from ..database import (
        get_latest_timestamp, get_latest_count,
        get_count_for_metric, get_latest_release_date
    )
    from ..storage import upload_file, download_file, StorageClient
except ImportError:
    from dispatch.database import (
        get_latest_timestamp, get_latest_count,
        get_count_for_metric, get_latest_release_date
    )
    from dispatch.storage import upload_file, download_file, StorageClient

try:
    from __app__.utilities.latest_data import get_timestamp_for
    from __app__.specimen_date_demographics.constants import NEXT_DEPLOYMENT_PATH as SPECIMEN_DEMOGRAPHY_PATH
    from __app__.publish_date_demographics.constants import NEXT_DEPLOYMENT_PATH as PUBLISH_DEMOGRAPHY_PATH
except ImportError:
    from utilities.latest_data import get_timestamp_for
    from specimen_date_demographics.constants import NEXT_DEPLOYMENT_PATH as SPECIMEN_DEMOGRAPHY_PATH
    from publish_date_demographics.constants import NEXT_DEPLOYMENT_PATH as PUBLISH_DEMOGRAPHY_PATH

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "application",
    "_run_submission"
]


CONTAINER_NAME = getenv("StorageContainerName")

LATEST_WEBSITE_UPLOAD_KWS = dict(
    container="publicdata",
    path="assets/dispatch/website_timestamp",
    content_type="text/plain; charset=utf-8",
    cache="no-cache, max-age=0"
)


LATEST_PUBLISHED_UPLOAD_KWS = dict(
    container="pipeline",
    path="info/latest_published",
    content_type="text/plain; charset=utf-8",
    cache="no-cache, max-age=0, stale-while-revalidate=300"
)


UNIVERSAL_HEADERS = {
    "Content-Encoding": "gzip",
    "server": "PHE API Service (Unix)",
    "strict-transport-security": "max-age=31536000; includeSubdomains; preload",
    "x-frame-options": "deny",
    "x-content-type-options": "nosniff",
    "x-xss-protection": "1; mode=block",
    "referrer-policy": "origin-when-cross-origin, strict-origin-when-cross-origin",
    "content-security-policy": "default-src 'none'; style-src 'self' 'unsafe-inline'",
    "x-phe-media-type": "PHE-COVID19.v1",
    "Cache-Control": "no-store"
}

API_ENV = getenv("API_ENV", "PRODUCTION")
STORAGE_CONN_STR = getenv("DeploymentBlobStorage", "")

EXPIRE_CACHE_IN = 1 * 60  # 4 minute in seconds

if API_ENV in ("PRODUCTION", "STAGING"):
    EXPIRE_CACHE_IN = 15 * 60  # 15 minute in seconds

DATE_FORMAT = r"%A, %d %B %Y at %H:%M%p GMT"

curr_dir = split(abspath(__file__))[0]

file_loader = FileSystemLoader(join(curr_dir, "templates"))
env = Environment(loader=file_loader)

specimen_demog_path = SPECIMEN_DEMOGRAPHY_PATH.format(filename="stacked")
publish_demog_path = PUBLISH_DEMOGRAPHY_PATH.format(filename="stacked")

partition_file_kws = dict(
    container="pipeline",
    path="info/seriesDate",
    compressed=False,
    content_type="plain/text"
)


def upload_legacy_files():
    try:
        from ..database import get_legacy_cases
    except ImportError:
        from dispatch.database import get_legacy_cases

    data = get_legacy_cases()
    filename = f"coronavirus-cases_latest.csv"
    client = StorageClient(
        container="downloads",
        path=f"csv/{filename}",
        content_type='text/csv; charset=utf-8',
        cache_control="no-store",
        content_disposition=f'attachment; filename="{filename}"'
    )
    client.upload(data)

    return True


def from_iso_timestamp(timestamp):
    if len(timestamp.split('.')[-1].strip("Z")) == 7:
        return datetime.fromisoformat(timestamp.replace("0Z", "").replace("5Z", ""))
    else:
        return datetime.fromisoformat(timestamp.replace("Z", ""))


def convert_timestamp(timestamp):
    if isinstance(timestamp, str):
        timestamp = from_iso_timestamp(timestamp)

    return timestamp.strftime(DATE_FORMAT)


def remaining_jobs(queue_name):
    qc: QueueClient = QueueClient.from_connection_string(
        STORAGE_CONN_STR,
        queue_name=queue_name,
        message_encode_policy=BinaryBase64EncodePolicy()
    )

    props = qc.get_queue_properties()

    return int(props.approximate_message_count)


def get_token_data():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from json import loads

    token = download_file('pipeline', 'dispatch/token.bin')

    key = getenv("TokenKey").encode()

    # Decrypt (raises InvalidTag for wrong key or corrupted input)
    data = AESGCM(key).decrypt(token[:12], token[12:], b"")

    return loads(data.decode())


def get_token_timestamp():
    token = get_token_data()

    return convert_timestamp(token['issue_timestamp'])


def update_service(**context) -> HttpResponse:
    try:
        latest_available = get_latest_timestamp()
    except Exception as e:
        logging.exception(e)
        logging.info(
            "Failed to extract latest timestamp from the database. "
            "Will extract from the file."
        )
        latest_available = context['latest_available']

    if API_ENV == "STAGING":
        upload_file(data=latest_available, **LATEST_PUBLISHED_UPLOAD_KWS)

        latest_released = datetime.utcnow().isoformat() + "Z"
        upload_file(data=latest_released, **LATEST_WEBSITE_UPLOAD_KWS)

        date = get_latest_release_date()
        with StorageClient(**partition_file_kws) as cli:
            cli.upload(date)

    template = env.get_template("success.html")

    upload_legacy_files()

    return HttpResponse(
        template.render({
            "expire_cache_in": f'{EXPIRE_CACHE_IN / 60:.2f}'
        }),
        mimetype="text/html"
    )


def _run_submission(**context) -> HttpResponse:
    try:
        latest_available = get_latest_timestamp()
    except Exception as e:
        logging.exception(e)
        logging.info(
            "Failed to extract latest timestamp from the database. "
            "Will extract from the file."
        )
        latest_available = context['latest_available']

    # latest_published = context['latest_published']
    # delta = from_iso_timestamp(latest_published) - from_iso_timestamp(latest_available)
    #
    # if delta.total_seconds() != 0:
    return update_service(**context)


def submission(request, **context) -> HttpResponse:
    token_data = get_token_data()

    if token_data['token'] == request.form['token']:
        return _run_submission(**context)
    
    return get_index(request, **context)


def get_demographic_counts():
    try:
        from __app__.demographic_etl.db_processors import DemographicsCategory, get_latest_count
    except ImportError:
        from demographic_etl.db_processors import DemographicsCategory, get_latest_count

    specimen_date_db_count = get_count_for_metric(
        DemographicsCategory.specimen_date_cases.output_metric
    )

    specimen_date_dt_count = get_latest_count(DemographicsCategory.specimen_date_cases)

    publish_date_db_count = get_count_for_metric(
        DemographicsCategory.publish_date_cases.output_metric
    )

    publish_date_dt_count = get_latest_count(DemographicsCategory.publish_date_cases)

    result = {
        "specimen_date": {
            "dataset_count": format(specimen_date_dt_count['count'], ","),
            "dataset_date": specimen_date_dt_count['lastUpdate'],
            "db_count": format(specimen_date_db_count, ","),
            "match": specimen_date_dt_count['count'] == specimen_date_db_count
        },
        "publish_date": {
            "dataset_count": format(publish_date_dt_count['count'], ","),
            "dataset_date": publish_date_dt_count['lastUpdate'],
            "db_count": format(publish_date_db_count, ","),
            "match": publish_date_dt_count['count'] == publish_date_db_count
        }
    }

    return result


def get_index(request, **context) -> HttpResponse:
    try:
        latest_available = get_latest_timestamp()
    except Exception as e:
        logging.exception(e)
        logging.info(
            "Failed to extract latest timestamp from the database. "
            "Will extract from the file."
        )
        latest_available = context['latest_available']

    latest_published = context['latest_published']
    latest_released = context['latest_released']
    delta = from_iso_timestamp(latest_published) - from_iso_timestamp(latest_available)
    logging.warning(delta)
    template = env.get_template("main.html")

    total_data = int(download_file(CONTAINER_NAME, "dispatch/total_records").decode())
    latest_db_count = int(get_latest_count())
    difference = total_data - latest_db_count
    response_params = {
        'demographics_specimen_date': convert_timestamp(
            timestamp=get_timestamp_for("pipeline", specimen_demog_path)
        ),
        'demographics_publish_date': convert_timestamp(
            timestamp=get_timestamp_for("pipeline", publish_demog_path)
        ),
        "latest_available": convert_timestamp(latest_available),
        "latest_released": convert_timestamp(latest_released),
        "latest_published": convert_timestamp(latest_published),
        "delta": delta,
        "is_different": delta.total_seconds() != 0,
        "auth_token_date": get_token_timestamp(),
        "total_data": format(total_data, ","),
        "latest_count": format(latest_db_count, ","),
        "db_count_difference": difference,
        "environment": API_ENV.upper(),
        "remaining_jobs": remaining_jobs("fanout"),
        "failed_jobs": remaining_jobs("fanout-poison"),
        "demographics": get_demographic_counts()
    }

    return HttpResponse(
        compress(template.render(**response_params).encode()),
        mimetype="text/html",
        headers=UNIVERSAL_HEADERS
    )


def application(request: HttpRequest, **context) -> HttpResponse:
    if request.method == "POST":
        return submission(request, **context)
    elif request.method == "GET":
        return get_index(request, **context)

    return HttpResponse(status_code=int(HTTPStatus.BAD_REQUEST), headers=UNIVERSAL_HEADERS)


if __name__ == '__main__':
    upload_legacy_files()