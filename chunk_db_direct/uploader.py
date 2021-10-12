#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple, Tuple
from os import getenv
import logging
from tempfile import TemporaryFile
from datetime import datetime

# 3rd party:
from pandas import DataFrame, to_datetime, read_parquet
from sqlalchemy import select, func, join, and_

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_etl.processors.rolling import change_by_sum
    from __app__.db_etl.homogenisation import homogenise_dates
    from __app__.db_etl_upload import (
        deploy_preprocessed_long, get_partition_id, create_partition
    )
    from __app__.db_tables.covid19 import (
        Session, ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from __app__.data_registration import set_file_releaseid
except ImportError:
    from storage import StorageClient
    from db_etl.processors.rolling import change_by_sum
    from db_etl.processors.homogenisation import homogenise_dates
    from db_tables.covid19 import (
        Session, ReleaseReference, AreaReference, MetricReference, ReleaseCategory
    )
    from db_etl_upload import (
        deploy_preprocessed_long, get_partition_id, create_partition
    )
    from data_registration import set_file_releaseid

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
]


RECORD_KEY = getenv("RECORD_KEY").encode()

processed_data_kws = dict(
    container="pipeline",
    content_type="application/octet-stream",
    compressed=False,
    cache_control="no-cache, max-age=0",
    tier='Cool'
)


class Payload(NamedTuple):
    data_path: str
    area_type: str
    timestamp: str
    process_name: str


def get_data(path: str, fp):
    with StorageClient(container="rawdbdata", path=path) as cli:
        cli.download().readinto(fp)

    fp.seek(0)


def get_dataset(payload: Payload) -> DataFrame:
    with TemporaryFile() as fp:
        get_data(payload.data_path, fp)
        result = read_parquet(fp)

    return result


def get_release_id(datestamp: datetime, process_name: str) -> Tuple[int, datetime]:
    """
    Generates or retrieves the `release_id` for the process.

    Parameters
    ----------
    datestamp : datetime
        Datestamp for the data.

    process_name : str
        Name of the process - must match the ENUM defined in the database.

    Returns
    -------
    Tuple[int, datetime]
        Tuple of `release_id` and the timestamp associated with the release.
    """
    query = (
        select([
            ReleaseReference.id,
            ReleaseReference.timestamp
        ])
        .select_from(
            join(
                ReleaseReference, ReleaseCategory,
                ReleaseReference.id == ReleaseCategory.release_id
            )
        )
        .where(
            and_(
                func.DATE(ReleaseReference.timestamp) == func.DATE(datestamp.isoformat()),
                ReleaseCategory.process_name == process_name
            )
        )
    )

    session = Session()

    try:
        response = session.execute(query)
        result = response.fetchone()

        if result is not None:
            return result

    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    try:
        release = ReleaseReference(timestamp=datestamp)
        session.begin()
        session.add(release)
        session.commit()

        category = ReleaseCategory(
            release_id=release.id,
            process_name=process_name
        )
        session.begin()
        session.add(category)
        session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return get_release_id(datestamp, process_name)


def convert(x):
    if not x:
        return {"value": None}

    try:
        int_x = int(x)
        if int_x == x:
            return {"value": int_x}
    except Exception:
        pass

    return {"value": x}


def process(data: DataFrame) -> DataFrame:
    data = data.melt(
        id_vars=["areaType", "areaCode", "date"],
        var_name="metric",
        value_name="payload"
    )
    data.payload = data.payload.map(convert)
    return data


def main(payload) -> str:
    logging.info(f"Processing: {payload}")

    payload = Payload(**payload)
    parsed_timestamp = datetime.fromisoformat(payload.timestamp)

    release_id, timestamp = get_release_id(parsed_timestamp, payload.process_name)
    set_file_releaseid(filepath=payload.data_path, release_id=release_id)

    create_partition(area_type=payload.area_type.lower(), release=timestamp)

    _ = (
        get_dataset(payload)
        .pipe(process)
        .pipe(
            lambda dt: dt.assign(
                date=to_datetime(dt.date, format="%Y-%m-%d"),
                release_id=release_id,
                partition_id=get_partition_id(
                    area_type=payload.area_type.lower(),
                    release=timestamp
                )
            )
        )
        .rename(columns={"areaType": "area_type", "areaCode": "area_code"})
        .pipe(deploy_preprocessed_long)
    )

    return f"DONE: {payload.data_path}"
