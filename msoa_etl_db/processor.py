#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple, Dict, NoReturn
from json import loads
from os import getenv
from functools import lru_cache
from datetime import datetime, timedelta
from hashlib import blake2s
import logging
from tempfile import TemporaryFile
from math import ceil

# 3rd party:
from pandas import DataFrame, date_range, read_parquet

from sqlalchemy.dialects.postgresql import insert

from numpy import array_split

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_etl.processors.rolling import change_by_sum
    from __app__.db_etl.homogenisation import homogenise_dates
    from __app__.db_tables.covid19 import Session, MainData
except ImportError:
    from storage import StorageClient
    from db_etl.processors.rolling import change_by_sum
    from db_etl.processors.homogenisation import homogenise_dates
    from db_tables.covid19 import Session, MainData, DB_INSERT_MAX_ROWS

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
    'dry_run'
]


RECORD_KEY = getenv("RECORD_KEY").encode()

processed_data_kws = dict(
    container="pipeline",
    content_type="application/octet-stream",
    compressed=False,
    cache_control="no-cache, max-age=0",
    tier='Cool'
)


class MSOAPayload(NamedTuple):
    data_path: Dict[str, str]
    area_code: str
    area_type: str
    metric: str
    partition_id: str
    population: int
    area_id: int
    metric_id: int
    release_id: int
    timestamp: str


def get_date_periods(start_date: str, end_date: str) -> DataFrame:
    start_date = datetime.fromisoformat(start_date)

    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    start_date = (start_date - timedelta(days=5)).date()

    periods = date_range(
        end=end_date,
        start=start_date,
        freq="-7D"
    )

    return periods.map(lambda x: x.strftime("%Y-%m-%d"))


def calculate_rolling_rate(df: DataFrame, metric: str) -> DataFrame:
    df[f"{metric}RollingRate"] = (
            df[f"{metric}RollingSum"] / df.population * 100_000
    ).round(1)

    return df


def to_periodic_data(df: DataFrame, timestamp: str) -> DataFrame:
    periods = get_date_periods(timestamp, df.date.min())

    return df.loc[df.date.isin(periods), :]


def convert_types(df: DataFrame, metric: str) -> DataFrame:
    df = (
        df
        .where(df.notnull(), None)
        .rename(columns={
            f'{metric}RollingSum': "rollingSum",
            f'{metric}Change': "change",
            f'{metric}Direction': "direction",
            f'{metric}ChangePercentage': "changePercentage",
            f'{metric}RollingRate': "rollingRate"
        })
        .to_dict("index")
    )

    return DataFrame(df.items(), columns=["date", "payload"])


def generate_row_hash(d: DataFrame, hash_only=False, date=None) -> DataFrame:
    hash_cols = [
        "date",
        "area_type",
        "area_code",
        "metric_id",
        "release_id"
    ]

    # Create hash
    hash_key = (
        d
        .loc[:, hash_cols]
        .astype(str)
        .sum(axis=1)
        .apply(str.encode)
        .apply(lambda x: blake2s(x, key=RECORD_KEY, digest_size=12).hexdigest())
    )

    if hash_only:
        return hash_key

    column_names = d.columns

    data = d.assign(
        hash=hash_key,
        seriesDate=date,
        id=hash_key
    ).loc[:, ['id', 'hash', 'seriesDate', *list(column_names)]]

    return data


def get_dataset(payload: MSOAPayload) -> DataFrame:
    with TemporaryFile() as fp, StorageClient(**payload.data_path) as client:
        client.download().readinto(fp)
        fp.seek(0)

        result = read_parquet(fp, columns=["areaCode", "date", payload.metric])

    max_date = result.date.max()
    area_data = result.loc[result.areaCode == payload.area_code, :]
    area_data.date = area_data.date.astype("datetime64").dt.strftime("%Y-%m-%d")

    if max_date in area_data.date:
        return area_data

    dates = date_range(
        start=datetime.strptime(area_data.date.max(), "%Y-%m-%d") + timedelta(days=1),
        end=max_date,
        freq='1D'
    )

    missing_values = [
        {"areaCode": payload.area_code, "date": f"{date:%Y-%m-%d}", payload.metric: 0}
        for date in dates
    ]

    return area_data.append(missing_values)


def to_sql(df: DataFrame) -> NoReturn:
    if df.size == 0:
        return None

    df_size = df.shape[0]
    n_chunks = ceil(df_size / DB_INSERT_MAX_ROWS)

    session = Session()
    connection = session.connection()
    try:
        for chunk in df.pipe(array_split, n_chunks):
            records = chunk.to_dict(orient="records")

            insert_stmt = insert(MainData.__table__).values(records)
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[MainData.hash, MainData.partition_id],
                set_={MainData.payload.name: insert_stmt.excluded.payload}
            )

            connection.execute(stmt)
            session.flush()

    except Exception as err:
        session.rollback()
        raise err

    finally:
        session.close()

    return None


def normaliser(df: DataFrame, column: str) -> DataFrame:
    df.loc[df.loc[:, column].isnull(), column] = 0
    return df


def suppress_by_rolling_sum(df: DataFrame, metric: str) -> DataFrame:
    metrics = [
        f'{metric}RollingSum',
        f'{metric}Change',
        f'{metric}Direction',
        f'{metric}ChangePercentage',
        f'{metric}RollingRate'
    ]

    df.loc[:, metrics] = (
        df
        .loc[:, metrics]
        .where(df[f'{metric}RollingSum'] > 2, None)
    )

    return df


@lru_cache()
def get_cached_dataset(**payload) -> DataFrame:
    payload["data_path"] = {
        "container": payload.pop("container"),
        "path": payload.pop("path")
    }
    return get_dataset(MSOAPayload(**payload))


def dry_run(payloadJson: str) -> DataFrame:
    """
    .. warning::
        Produces unsuppressed data.
    """
    payload_dict = loads(payloadJson)
    payload = MSOAPayload(**payload_dict)
    data_path = payload_dict.pop("data_path")

    data = (
        get_cached_dataset(**payload_dict, **data_path)
        .sort_values(["date"])
        .assign(areaType="msoa")
        .pipe(homogenise_dates)
        .pipe(normaliser, column=payload.metric)
        .assign(population=payload.population)
        .pipe(change_by_sum, metrics=[payload.metric], min_sum_allowed=0, min_sum_sub=0)
        .pipe(calculate_rolling_rate, metric=payload.metric)
        .drop(columns=["areaType", "areaCode", "population"])
        .set_index("date")
        .assign(
            area_code=payload.area_code,
            area_type=payload.area_type
        )
    )

    return data.where(data.notnull(), None)


def main(payload) -> str:
    logging.info(f"Processing: {payload}")

    payload = MSOAPayload(**payload)

    to_sql(
        get_dataset(payload)
        .sort_values(["date"])
        .assign(areaType=payload.area_type)
        .pipe(homogenise_dates)
        .pipe(normaliser, column=payload.metric)
        .assign(population=payload.population)
        .pipe(change_by_sum, metrics=[payload.metric], min_sum_allowed=3, min_sum_sub=2)
        .pipe(calculate_rolling_rate, metric=payload.metric)
        .pipe(suppress_by_rolling_sum, metric=payload.metric)
        .pipe(to_periodic_data, timestamp=payload.timestamp)
        .drop(columns=["areaType", "areaCode", "population", payload.metric])
        .set_index("date")
        .pipe(convert_types, metric=payload.metric)
        .assign(
            area_id=payload.area_id,
            release_id=payload.release_id,
            metric_id=payload.metric_id,
            area_code=payload.area_code,
            partition_id=payload.partition_id,
            area_type=payload.area_type
        )
        .pipe(lambda d: d.assign(hash=generate_row_hash(d, hash_only=True)))
        .drop(columns=["area_type", "area_code"])
    )

    return f"DONE: {payload.area_code}"

