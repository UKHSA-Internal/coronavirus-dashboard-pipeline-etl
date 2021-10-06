#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from typing import NoReturn, Dict
from json import loads, dumps
from io import BytesIO

# 3rd party:
from pandas import concat, read_feather, DataFrame

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.storage.etl_utils import TestOutput, MainOutput
    from __app__.db_etl.token import generate_token
    from __app__.utilities.generic_types import ArchivePayload
except ImportError:
    from storage import StorageClient
    from storage.etl_utils import TestOutput, MainOutput
    from db_etl.token import generate_token
    from utilities.generic_types import ArchivePayload

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV", "DEVELOPMENT")

# DEBUG = True and not ENVIRONMENT == "PRODUCTION"
DEBUG = getenv("DEBUG", False)

CONTAINER_NAME = getenv("StorageContainerName")
STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
RAW_DATA_CONTAINER = "rawdbdata"
TARGET_CONTAINER = getenv("StorageContainerName")

PROCESSED_FILES_KWS = dict(
    container="pipeline",
    content_type="application/octet-stream",
    compressed=False,
    cache_control="no-cache, max-age=0",
    tier='Cool'
)


def load_data(etl_data: Dict[str, str]) -> DataFrame:
    path = etl_data['path']
    logging.info(f"> Downloading data from '{path}'")

    with StorageClient(**PROCESSED_FILES_KWS, path=path) as client:
        if not client.exists():
            raise RuntimeError(f"Blob does not exist: {path}")

        data = client.download()

        logging.info(f"> Download complete")

        data_io = BytesIO(data.readall())
        data_io.seek(0)

    return read_feather(data_io, use_threads=False)


def main(payload) -> NoReturn:
    logging.info(f"Archiver triggered with payload: {payload}")

    output_obj = MainOutput if not DEBUG else TestOutput

    payload = ArchivePayload(**payload)

    if payload.environment != "PRODUCTION":
        return f"No archive - environment: {payload.environment}"

    data = concat(map(load_data, payload.results))

    date, _ = payload.timestamp.split("T")

    csv_data = data.to_csv(index=False)
    archive_csv_output = output_obj(f"archive/processed_{date}.csv")
    archive_csv_output.set(csv_data, content_type="text/csv; charset=utf-8")
    logging.info(f'\tStored CSV sample')

    timestamp_output = output_obj(f"info/latest_available")
    release_ts = data.releaseTimestamp.unique()[0]
    timestamp_output.set(release_ts, content_type="text/plain; charset=utf-8")
    logging.info(f'\tStored timestamp')

    data_length = max(data.shape)
    total_records = output_obj(f"dispatch/total_records")
    total_records.set(str(data_length))

    if not DEBUG:
        with StorageClient(container=RAW_DATA_CONTAINER, path=payload.original_path) as client:
            client.copy_blob(
                target_container=TARGET_CONTAINER,
                target_path=f"archive/processed_{date}.json"
            )
        logging.info(f'\tOriginal data copied into archives')

    logging.info(f'\tData have been archived.')

    return f"Archiver: DONE - {payload}"


if __name__ == '__main__':
    from json import dumps
    from sys import stdout

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    archive_payload = dict(
        results=list(),
        original_path="data_202101141610.json",
        timestamp="2021-01-04T13:41:50.3362875Z"
    )

    # with StorageClient(container="pipeline", path="etl/processed/2021-01-04") as cli:
    #     for blob in cli:
    #         archive_payload['results'].append({"path": blob['name']})
    #
    # d = main(dumps(archive_payload))

    # print(d.head().to_string())
    # print(d.shape)

    path = "etl/transit/2021-01-14_1801/nhsTrust_N4H3U.json"
    dt = load_data({"path": path})

    print(dt.to_string())

