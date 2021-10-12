#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv
from functools import lru_cache
from hashlib import blake2s
from io import BytesIO
from datetime import datetime
from json import loads
import logging

# 3rd party:
from sqlalchemy.dialects.postgresql import insert, dialect as postgres
from sqlalchemy.exc import ProgrammingError

from pandas import read_feather, to_datetime, DataFrame, read_sql

from numpy import NaN, ndarray, array_split

from azure.core.exceptions import ResourceNotFoundError

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import (
        Session, MainData, ReleaseReference,
        AreaReference, MetricReference, DB_INSERT_MAX_ROWS
    )
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import (
        Session, MainData, ReleaseReference,
        AreaReference, MetricReference, DB_INSERT_MAX_ROWS
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
    'generate_row_hash',
    'to_sql',
    'deploy_preprocessed',
    'get_partition_id',
    'get_release',
    'create_partition',
    'deploy_preprocessed_long'
]

RECORD_KEY = getenv("RECORD_KEY").encode()


def trim_sides(data):
    for metric in data.metric.dropna().unique():
        dm = (
            data
            .loc[data.metric == metric, :]
            .sort_values(["date"], ascending=True)
        )

        if not dm.payload.dropna().size:
            continue

        try:
            cumsum = dm.payload.abs().cumsum()
            first_nonzero = cumsum.loc[cumsum > 0].index[0]
        except (TypeError, IndexError):
            first_nonzero = dm.payload.first_valid_index()

        dm.loc[:first_nonzero + 1] = NaN

        if not dm.payload.dropna().size:
            continue

        last_valid = dm.payload.last_valid_index()

        dm.loc[last_valid - 1:, :] = NaN
        data.loc[dm.index] = dm

    return data.dropna(how="all", axis=0)


def get_area_data():
    session = Session()
    try:
        results = read_sql(
            f"""\
            SELECT c.id AS "area_id", c.area_type, c.area_code
            FROM covid19.area_reference AS c
            """,
            con=session.connection(),
            index_col=["area_type", "area_code"]
        )
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return results


nested = {
    "newAdmissionsByAge",
    "cumAdmissionsByAge",
    "femaleCases",
    "maleCases",
    "femaleNegatives",
    "maleNegatives",
    "newCasesBySpecimenDateAgeDemographics",
    "newDeaths28DaysByDeathDateAgeDemographics"
}

id_vars = [
    'area_type',
    'area_code',
    'partition_id',
    'date',
    'release_id',
    'hash'
]


metric_names = {
    'areaType': 'area_type',
    'areaCode': 'area_code',
    'areaName': 'area_name',
    'releaseTimestamp': 'release_timestamp',
}


def generate_row_hash(d: DataFrame, hash_only=False, date=None) -> DataFrame:
    """

    Parameters
    ----------
    d
    hash_only
    date

    Returns
    -------

    """
    hash_cols = [
        "date",
        "area_type",
        "area_code",
        "metric_id",
        "release_id"
    ]

    try:
        d.date = d.date.map(lambda x: x.strftime("%Y-%m-%d"))
    except AttributeError:
        pass

    d.date = d.date.map(lambda x: x[:10])

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


def get_metrics():
    session = Session()

    try:
        metrics = read_sql(
            f"""\
            SELECT ref.id AS "metric_id", ref.metric
            FROM covid19.metric_reference AS ref;
            """,
            con=session.connection(),
            index_col=["metric"]
        )
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return metrics


@lru_cache(256)
def get_release(timestamp):
    insert_stmt = (
        insert(ReleaseReference.__table__)
        .values(timestamp=timestamp)
    )

    stmt = (
        insert_stmt
        .on_conflict_do_update(
            index_elements=[ReleaseReference.timestamp],
            set_={ReleaseReference.timestamp.name: insert_stmt.excluded.timestamp}
        )
        .returning(ReleaseReference.id)
    )

    # session = Session(autocommit=True)
    session = Session()
    try:
        response = session.execute(stmt)
        result = response.fetchone()[0]
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return result


def get_partition_id(area_type, release):
    if area_type in ["nhsTrust", "utla", "ltla", "msoa"]:
        partition_id = f"{release:%Y_%-m_%-d}|{area_type.lower()}"
    else:
        partition_id = f"{release:%Y_%-m_%-d}|other"

    return partition_id


def create_partition(area_type: str, release: datetime):
    """
    Creates new database partition - if one doesn't already exist - for
    the `time_series` table based on `area_type` and `release` datestamp.

    Parameters
    ----------
    area_type : str
        Area type, as defined in the `area_reference` table.

    release: datetime
        Release timestamp of the data.

    Returns
    -------
    NoReturn
    """
    partition_id = get_partition_id(area_type, release)

    if area_type in ["nhsTrust", "utla", "ltla", "msoa"]:
        area_partition = f"{release:%Y_%-m_%-d}_{area_type.lower()}"
    else:
        area_partition = f"{release:%Y_%-m_%-d}_other"

    session = Session()
    try:
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS covid19.time_series_p{area_partition} 
            PARTITION OF covid19.time_series ( partition_id )
            FOR VALUES IN ('{partition_id}');
            """
        )
        session.flush()
    except ProgrammingError as e:
        session.rollback()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()


def sql_fn(row):
    return MainData(**row.to_dict())


def to_sql(df: DataFrame):
    if df.size == 0:
        return None

    df_size = df.shape[0]
    n_chunks = df_size // DB_INSERT_MAX_ROWS + 1
    df.drop_duplicates(
        ["release_id", "area_id", "metric_id", "date"],
        keep="first",
        inplace=True
    )

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


def validate_metrics(dt):
    metrics = get_metrics()

    invalid_metrics = set(dt.metric).difference(metrics.index)

    if len(invalid_metrics):
        for metric in invalid_metrics:
            add_metric(metric)

        return dt.join(get_metrics(), on=["metric"])

    return dt.join(metrics, on=["metric"])


def deploy_nested(df, key):
    if key not in df.columns:
        return df

    dt = df.loc[:, ["date", "area_type", "area_code", "partition_id", "release_id", key]]
    if dt.size:
        try:
            dt.drop(columns=set(dt.columns).difference([*id_vars, key]), inplace=True)
            dt.rename(columns={key: "payload"}, inplace=True)
            dt.dropna(subset=["payload"], inplace=True)
            dt.payload = dt.payload.map(lambda x: list(x) if not isinstance(x, dict) else list())

            to_sql(
                dt
                .assign(metric=key)
                .join(get_area_data(), on=["area_type", "area_code"])
                .pipe(validate_metrics)
                .pipe(lambda d: d.assign(hash=generate_row_hash(d, hash_only=True)))
                .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
            )
        except Exception as e:
            raise e

    return df


def deploy_preprocessed_long(df):
    """
    Generates hash key and deploys the data to the database.

    Data must be preprocessed and in long (unstacked) format.

    Parameters
    ----------
    df: DataFrame
        Dataframe containing the following columns:

        - metric_id
        - area_id
        - partition_id
        - release_id
        - date
        - payload

    Returns
    -------
    DataFrame
        Processed dataframe
    """
    to_sql(
        df
        .join(get_area_data(), on=["area_type", "area_code"])
        .pipe(validate_metrics)
        .pipe(lambda d: d.assign(hash=generate_row_hash(d, hash_only=True)))
        .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
    )

    return df


def deploy_preprocessed(df, key):
    df.loc[:, key] = df.loc[:, key].map(list)

    to_sql(
        df
        .rename(columns={key: "payload"})
        .assign(metric=key)
        .join(get_area_data(), on=["area_type", "area_code"])
        .pipe(validate_metrics)
        .pipe(lambda d: d.assign(hash=generate_row_hash(d, hash_only=True)))
        .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
    )

    return df


def convert_timestamp(dt: DataFrame, timestamp):
    if "release_timestamp" in dt.columns:
        ts = dt.loc[:, "release_timestamp"].unique()[0]

        if ts == timestamp and not isinstance(ts, str):
            return dt

        dt.drop(columns=["release_timestamp"], inplace=True)

    dt = dt.assign(release_timestamp=timestamp.isoformat() + "Z")
    dt.loc[:, "release_timestamp"] = to_datetime(dt.release_timestamp)

    return dt


def format_weekly_metrics(df: DataFrame) -> DataFrame:
    extras = [
        'weeklyPeopleVaccinatedFirstDoseByVaccinationDate',
        'weeklyPeopleVaccinatedSecondDoseByVaccinationDate',
        'alertLevel',
        'transmissionRateMin',
        'transmissionRateMax',
        'transmissionRateGrowthRateMin',
        'transmissionRateGrowthRateMax',
    ]

    weekly_metrics = (
        df
        .metric[
            (
                (df.metric.str.contains("weekly", case=False, regex=False)) |
                (df.metric.isin(extras))
            )
        ]
        .unique()
    )

    if not len(weekly_metrics):
        return df

    df.loc[df.metric.isin(weekly_metrics), :] = (
        df
        .loc[df.metric.isin(weekly_metrics), :]
        .dropna(subset=["payload"], how="any", axis=0)
    )

    return df


def add_metric(metric):
    stmt = (
        insert(MetricReference.__table__)
        .values(metric=metric)
        .on_conflict_do_nothing(index_elements=[MetricReference.metric])
        .compile(dialect=postgres())
    )

    session = Session()
    try:
        # session.begin()
        session.connection().execute(stmt)
        session.flush()
        # session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def convert_values(value):
    if isinstance(value, ndarray):
        return list(value)

    if not isinstance(value, (dict, list)):
        return {"value": value}

    return value


def confirm_or_create_area(area_type: str, area_code: str, area_name: str):
    stmt = (
        insert(AreaReference.__table__)
        .values(
            area_type=area_type,
            area_code=area_code,
            area_name=area_name,
            unique_ref=f"{area_type}|{area_code}"
        )
        .on_conflict_do_nothing(
            index_elements=[
                AreaReference.area_type,
                AreaReference.area_code
            ]
        )
        .compile(dialect=postgres())
    )

    # session = Session(autocommit=True)
    session = Session()
    try:
        session.connection().execute(stmt)
        session.flush()
        # session.begin()
        # session.add(stmt)
        # session.commit()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def deploy(fp: BytesIO, timestamp: datetime, area_type: str,
           container: str, filepath: str):
    try:
        df = read_feather(fp)
    except Exception as e:
        logging.exception(e)
        raise e

    demog_only = {
        "newDeaths28DaysByDeathDateAgeDemographics",
        "newCasesBySpecimenDateAgeDemographics",
        "newCasesByPublishDateAgeDemographics",
    }

    preprocessed = {
        "vaccinationsAgeDemographics",
    }

    area_code = df.areaCode.unique()[0]

    try:
        area_name = df.areaName.unique()[0]

        if area_name in ["Northern Ireland", "Scotland", "England", "Wales"]:
            area_type = "nation"
            df.areaType = area_type

        confirm_or_create_area(
            area_type=area_type,
            area_code=area_code,
            area_name=area_name
        )

    except AttributeError:
        # It's not a trust - i.e. likely the demogs.
        # This needs to change in the future.
        pass

    try:
        d = (
            df
            .drop(columns=["id", "seriesDate", "areaName", "areaNameLower", "releaseTimestamp", "hash"], errors="ignore")
            .pipe(lambda dt: dt.assign(
                date=to_datetime(dt.date, format="%Y-%m-%d"),
                release_id=get_release(timestamp.isoformat() + "Z"),
                partition_id=get_partition_id(area_type=area_type, release=timestamp)
            ))
            .pipe(lambda dt: dt.rename(columns=metric_names))
        )

        for metric_name in demog_only:
            if metric_name not in df.columns:
                continue

            _ = deploy_nested(d, metric_name)

            return None

        for metric_name in preprocessed:
            if metric_name not in df.columns:
                continue

            _ = deploy_preprocessed(d, metric_name)

            return None

        d = (
            d
            .melt(
                id_vars=['area_type', 'area_code', 'date', 'release_id', 'partition_id'],
                var_name="metric",
                value_name="payload"
            )
            .pipe(validate_metrics)
            .pipe(trim_sides)
            .pipe(format_weekly_metrics)
        )

        # d.payload.replace({"UP": 1, "DOWN": -1, "SAME": 0}, inplace=True)
        d.payload = d.payload.where(d.payload.notnull(), None)
        d.payload = d.payload.map(convert_values)

        unique_metrics = d.loc[:, "metric"].unique()
        for metric_name in ["maleCases", "femaleCases"]:
            if metric_name not in unique_metrics:
                continue

            d.loc[d.metric == metric_name, "payload"] = (
                d
                .loc[d.metric == metric_name, "payload"]
                .map(lambda x: x if isinstance(x, list) else list())
            )

        to_sql(
            d
            .dropna(
                subset=["metric", "area_type", "area_code", "release_id", "date"],
                how="any"
            )
            .join(get_area_data(), on=["area_type", "area_code"])
            .pipe(lambda dt: dt.assign(hash=generate_row_hash(dt, hash_only=True)))
            .loc[:, ["metric_id", "area_id", "partition_id", "release_id", "hash", "date", "payload"]]
        )

        return None

    except Exception as e:
        logging.exception(e)
        logging.critical(f"EXCEPTION: {container}/{filepath}")
        raise e


def download_file(container: str, path: str) -> BytesIO:
    logging.info(f"> Downloading data from '{container}/{path}'")

    with StorageClient(container=container, path=path) as client:
        data = client.download()

    logging.info(f"> Download complete")

    fp = BytesIO(data.readall())
    fp.seek(0)

    return fp


def get_timestamp(datestamp) -> datetime:
    with StorageClient(container="publicdata", path="assets/dispatch/dates.json") as client:
        timestamp = loads(client.download().readall().decode())[datestamp]

    ts = datetime.fromisoformat(timestamp.replace("5Z", ""))
    logging.info(f"> timestamp extracted {ts}")
    return ts


def main(payload: dict):
    filepath = payload["file_path"]

    container = "pipeline"

    logging.info(f"Starting to process  '{container}/{filepath}' for deployment to PGSQL.")

    if "null" in filepath:
        return f"INVALID: {filepath}"

    timestamp = datetime.fromisoformat(payload["timestamp"])
    area_type = filepath.split("/")[-1].split("_")[0]

    create_partition(area_type=area_type, release=timestamp)

    try:
        fp = download_file(container, filepath)
    except ResourceNotFoundError:
        logging.warning(f'Blob not found: "{container}/{filepath}"')
        return f"NOT FOUND: {filepath}"

    deploy(fp, timestamp, area_type, container, filepath)

    return f"SUCCESS: {filepath} | {payload['timestamp']}"


# if __name__ == "__main__":
#     from datetime import timedelta
#
#     main({
#         "timestamp": (datetime.now() - timedelta(days=1)).isoformat(),
#         "file_path": ""
