#!/usr/bin python3

# # This commented out section was used in the local development
# import pathlib
# import site

# test_dir = pathlib.Path(__file__).resolve().parent
# root_path = test_dir.parent
# site.addsitedir(root_path)

import logging
from collections import namedtuple
from datetime import date, datetime, timedelta
from hashlib import blake2s
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from typing import Union

# According to Azure docs it might be needed to import the modules using '__app__'
try:
    from __app__.db_tables.covid19 import MainData, MetricReference, Session
    from __app__.storage import StorageClient
except ImportError:
    from db_tables.covid19 import MainData, MetricReference, Session
    from storage import StorageClient


__all__ = [
    'main'
]


METRIC_IDS_QUERY="""\
SELECT metric, id
FROM covid19.metric_reference
WHERE metric = ANY('{metrics_string}');\
"""
VACCINATIONS_QUERY = """\
SELECT partition_id, release_id, area_id, date, payload
FROM (
        SELECT *
        FROM covid19.time_series_p{partition}_other AS tsother
                JOIN covid19.release_reference AS rr ON rr.id = release_id
                JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                JOIN covid19.area_reference AS ar ON ar.id = tsother.area_id
        WHERE metric = 'vaccinationsAgeDemographics'
        AND date > ( DATE('{date}'))
        UNION
        (
            SELECT *
            FROM covid19.time_series_p{partition}_utla AS tsutla
                    JOIN covid19.release_reference AS rr ON rr.id = release_id
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                    JOIN covid19.area_reference AS ar ON ar.id = tsutla.area_id
            WHERE metric = 'vaccinationsAgeDemographics'
            AND date > ( DATE('{date}'))
        )
        UNION
        (
            SELECT *
            FROM covid19.time_series_p{partition}_ltla AS tsltla
                    JOIN covid19.release_reference AS rr ON rr.id = release_id
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
                    JOIN covid19.area_reference AS ar ON ar.id = tsltla.area_id
            WHERE metric = 'vaccinationsAgeDemographics'
            AND date > ( DATE('{date}'))
        )
    ) AS tsltla
ORDER BY date DESC;\
"""
# The list of nested metrics that should be converted to regular ones
METRICS_TO_CONVERT = [
    "cumPeopleVaccinatedAutumn22ByVaccinationDate",
    "cumVaccinationAutumn22UptakeByVaccinationDatePercentage",
]

# The data that comes from the database is saved as a named tuple (1 DB row -> 1 tuple)
TimeSeriesData = namedtuple(
    'TimeSeriesData',
    ['partition_id', 'release_id', 'area_id', 'date', 'payload']
)
# The structure that is saved into the DB, the order here does matter
TimeSeriesDataToDB = namedtuple(
    'TimeSeriesDataToDB',
    ['hash', 'release_id', 'area_id', 'metric_id', 'partition_id', 'date', 'payload']
)
# The list of data that we're intrested in.
# The items are the age ranges used in the nested metrics.
age_range_suffix = ['50+']
# The dict where the key is the metric, and its value is the metric ID
# It's used to keep already fetched metric IDs so they can be reused
# (no need to query the DB again)
metric_ids_collected = dict()
# This is used only for logging what new metrics were created
new_metric_created = list()


def to_sql(data: list):
    """
    This saves data to DB (time_series table)

    :param data: a list of tuples, a tuple contains valus for a row
    """
    session = Session()
    connection = session.connection()

    logging.info("Writing/updating nested metrics to DB")

    try:
        for row in data:
            insert_statement = insert(MainData.__table__).values(row)
            statement = insert_statement.on_conflict_do_update(
                index_elements=[MainData.hash, MainData.partition_id],
                set_=dict(payload=insert_statement.excluded.payload)
            )

            connection.execute(statement)
            session.flush()

    except Exception as err:
        session.rollback()
        raise err

    finally:
        session.close()


def from_sql(partition: str, date: date):
    """
    This gets all the vaccination data needed from the DB

    :param partition: it's a 'partition_id' used in the SQL query string
    :param date: the earliest date we're intrested in

    :return: values from DB
    :rtype: list
    """
    session = Session()
    connection = session.connection()

    values_query = VACCINATIONS_QUERY.format(partition=partition, date=date)

    try:
        resp = connection.execute(
            text(values_query),
        )
        values = [TimeSeriesData(*record) for record in resp.fetchall()]
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return values


def get_or_create_new_metric_id(metric: str):
    """
    Based on the metric provided it returns it's ID. It tries to get it from the DB
    It eventually creates a new metric in 'metric_references' table

    :params metric: the metric
    :return: the metric ID
    :rtype: int
    """
    # if the metric ID is already known, then use it
    if metric_ids_collected.get(metric):
        return metric_ids_collected[metric]

    session = Session()
    connection = session.connection()

    obj = None

    # Try to get the metric ID from the DB
    try:
        resp = connection.execute(
            select(MetricReference.id).where(MetricReference.metric == metric)
        )
        obj = resp.fetchone()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    # If the metric doesn't exist in the DB, then create it
    if not obj or (obj and not obj[0]):
        try:
            INSERT_NEW_METRIC_QUERY = (
                "INSERT INTO covid19.metric_reference(metric, released, source_metric) "
                f"VALUES ('{metric}', false, false) "
                "RETURNING id;"
            )

            id = connection.execute(INSERT_NEW_METRIC_QUERY).first()[0]
            session.flush()
            new_metric_created.append(metric)
        except Exception as err:
            session.rollback()
            raise err
        finally:
            session.close()

        metric_ids_collected[metric] = id

        return id

    metric_ids_collected[metric] = obj[0]

    return obj[0]


def convert_values(data: list):
    """
    This creates a list of new metric data that can be saved into the DB

    :param data: list of nested metric data from the DB
    :return: the data to be saved into the DB
    :rtype: list
    """
    new_metric_data = []
    new_hash_keys = set()

    for row in data:
        for values in row.payload:
            if values.get('age') is None or values['age'] not in age_range_suffix:
                continue

            for metric in METRICS_TO_CONVERT:
                metric_id = get_or_create_new_metric_id(metric + values['age'])
                enc_string = (
                    # The order here makes difference, as any change to it
                    f"{str(row.release_id)}{str(row.area_id)}{str(metric_id)}"
                    f"{str(row.partition_id)}{row.date.isoformat()}"
                )
                hash = blake2s(digest_size=12)
                hash.update(enc_string.encode('UTF-8'))
                hash_string = hash.hexdigest()
                new_hash_keys.add(hash_string)

                new_metric = TimeSeriesDataToDB(
                    hash_string,
                    row.release_id,
                    row.area_id,
                    metric_id,
                    row.partition_id,
                    row.date,
                    {'value': values[metric]}
                )
                new_metric_data.append(new_metric)

    logging.info(f"Number of new hash keys: {len(new_hash_keys)}")
    logging.info(f"These new metrics have been created: {new_metric_created}")

    return new_metric_data


def get_latest_published():
    """
    This downloads 'latest_published' file from the blob storage
    and returns its content converted into datetime object

    :return: the timestamp of the latest release
    :rtype: datetime
    """
    path = 'info/latest_published'

    with StorageClient(container="pipeline", path=path) as cli:
        date = cli.download().read()

    date_string = date.decode("UTF-8")
    date_string = date_string.replace('Z', '')

    # for some reasons fromisoformat() expects milliseconds as 6 digits
    if len(date_string) > 26:
        date_string = date_string[:26]

    return datetime.fromisoformat(date_string)


def simple_db_query(query: str):
    """
    General purpose function to retrieve some data from the DB

    :param query: SQL query string
    :return: values from the DB
    :rtype: list
    """
    session = Session()
    connection = session.connection()

    try:
        resp = connection.execute(
            text(query)
        )
        values = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return values


# The 'input' is not used ATM, it might be removed with the next changes
def main(rawtimestamp: str) -> str:
    """ The function to create new metrics (if necessary) listed in METRICS_TO_CONVERT
        constant. If they already exist, it will only retrieve their IDs.
        It collects relevant data, transforms it and then saves it (the new metrics
        will be concatenation of the nested metric name and the age range).
        Nothing will be removed.

        :param input: timestamp passed from the orchestrator function
        :return: message that the function is completed
        :rtype: str
    """
    logging.info("Starting working on the nested metrics, 'rawtimestamp': {rawtimestamp}")

    if not rawtimestamp:
        return "No timestamp was provided for the nested metrics converter"

    # Getting the date -------------------------------------------------------------------
    ts = datetime.fromisoformat(rawtimestamp[:26])
    datestamp = date(year=ts.year, month=ts.month, day=ts.day)

    partition = f"{datestamp:%Y_%-m_%-d}"
    logging.info(f"The partition id (date related part): {partition}")


    # TODO: Because of the differences in the DBs, for now it won't work for all of them
    # # Get dates of the 2 latest releases -------------------------------------------------
    # query = (
    #     "SELECT DISTINCT(timestamp::DATE) "
    #     "FROM covid19.release_reference "
    #     "ORDER BY timestamp DESC "
    #     "LIMIT 2;"
    # )
    # date_list = simple_db_query(query)
    # latest_release_date = date_list[0][0]
    # previous_release_date = date_list[1][0]
    # logging.info(f"Fetched release dates: {latest_release_date}, {previous_release_date}")


    # Retrieving data (since the previous release) ---------------------------------------
    # TODO: This will be used when the DBs will have the same release day
    # values = from_sql(partition, previous_release_date - timedelta(days=1))
    values = from_sql(partition, datestamp - timedelta(days=10))
    logging.info(f"Retrieved {len(values)} rows from DB for nested metrics converter")


    # Preparing the data for saving back to the DB ---------------------------------------
    new_list = convert_values(values)
    logging.info(f"Number of new nested metric rows to save/update: {len(new_list)}")


    # Saving data ------------------------------------------------------------------------
    to_sql(new_list)
    logging.info("All converted nested metrics have been saved to DB")

    return f"Process converting the nested metrics has compelted: {datestamp}"


# This is not needed for prod, but useful for local development
# if __name__ == '__main__':
#     main("2022-12-28T15:15:15.123456")
