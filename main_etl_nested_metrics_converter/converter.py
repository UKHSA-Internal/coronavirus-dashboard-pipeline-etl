#!/usr/bin python3

# # This commented out section was used in the local development
# import pathlib
# import site

# test_dir = pathlib.Path(__file__).resolve().parent
# root_path = test_dir.parent
# site.addsitedir(root_path)

import json
import logging
from collections import namedtuple
from datetime import date, datetime
from hashlib import blake2s
from os import getenv
from sqlalchemy import column, select, text
from sqlalchemy.dialects.postgresql import insert

# According to Azure docs it might be needed to import the modules using '__app__'
try:
    from __app__.db_tables.covid19 import MainData, MetricReference, Session
    from __app__.main_etl_nested_metrics_converter import queries
except ImportError:
    from db_tables.covid19 import MainData, MetricReference, Session
    from main_etl_nested_metrics_converter import queries


__all__ = [
    'main'
]


RECORD_KEY = getenv("RECORD_KEY", "").encode()


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
# The data that we're intrested in.
age_metric_mapping = {
    # '50+': [
    #     "cumPeopleVaccinatedAutumn22ByVaccinationDate",
    #     "cumVaccinationAutumn22UptakeByVaccinationDatePercentage",
    # ],
    '65+': [
        "newPeopleVaccinatedAutumn23ByVaccinationDate",
        "cumPeopleVaccinatedAutumn23ByVaccinationDate",
        "cumVaccinationAutumn23UptakeByVaccinationDatePercentage",
    ],
    '75+': [
        "newPeopleVaccinatedSpring23ByVaccinationDate",
        "cumPeopleVaccinatedSpring23ByVaccinationDate",
        "cumVaccinationSpring23UptakeByVaccinationDatePercentage",
    ],
}
# The list of nested metrics that should be converted to regular ones
METRICS_TO_CONVERT = []
list(map(METRICS_TO_CONVERT.extend, age_metric_mapping.values()))

# This will be used to convert 'suffix' (it will be appended to a metric)
# not to has any problematic characters
suffix_mapping = {
    # '50+': '50plus',
    '65+': '65plus',
    '75+': '75plus',
}
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

    # These 3 are used only for logging
    done_10th = 1
    count = 0
    all_rows = len(data)

    logging.info(f"Writing/updating nested metrics to DB ({all_rows} rows)")
    dates = set()

    try:
        for row in data:
            insert_statement = insert(MainData.__table__).values(row)
            statement = insert_statement.on_conflict_do_update(
                index_elements=[MainData.hash, MainData.partition_id],
                set_=dict(payload=insert_statement.excluded.payload)
            )

            connection.execute(statement)
            session.flush()

            count += 1
            dates.add(str(row.date))

            if count >= all_rows / 10 * done_10th:
                logging.info(f"Nested metrics, rows saved: {count} ({done_10th * 10}%)")
                done_10th += 1

    except Exception as err:
        session.rollback()
        raise err

    finally:
        session.close()

    logging.info(f"The new data covers these dates: {dates}")


def from_sql(partition: str, cutoff_date: datetime):
    """
    This gets all the vaccination data needed from the DB

    :param partition: it's a 'partition_id' used in the SQL query string
    :param cutoff_date: this defines the oldest data we're insteredted in

    :return: values from DB
    :rtype: list
    """
    session = Session()
    connection = session.connection()

    # Temporarily increase max_intermediate_result_size value from 2GB to 4GB
    connection.execute(
        text("set citus.max_intermediate_result_size to 4194304")
    )

    try:
        query = queries.VACCINATIONS_QUERY.format(
            partition=partition,
            date=cutoff_date,
        )

        resp = connection.execute(
            text(query),
        )
        values = [TimeSeriesData(*record) for record in resp.fetchall()]
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return values


def get_or_create_new_metric_id(metric: str, age: str):
    """
    Based on the metric provided it returns it's ID. It tries to get it from the DB
    It eventually creates a new metric in 'metric_references' table

    :params metric: the metric
    :params age: age range
    :return: the metric ID
    :rtype: int
    """
    # the metric is concatenation of nested metric and the age range
    metric += suffix_mapping[age]
    # if the metric ID is already known, then use it
    if metric_ids_collected.get(metric):
        return metric_ids_collected[metric]

    session = Session()
    connection = session.connection()

    obj = None

    # Try to get the metric ID from the DB
    try:
        resp = connection.execute(
            select([MetricReference.id]).where(MetricReference.metric == metric)
        )
        obj = resp.fetchone()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    # If the metric doesn't exist in the DB, then create it
    if not obj or not hasattr(obj, 'id'):
        try:
            statement = (
                insert(MetricReference.__table__)
                .returning(column('id'))
                .values(metric=metric, released=True, source_metric=False)
                # .on_conflict_do_nothing(index_elements=['metric'])
            )

            id = connection.execute(statement).scalar()
            session.flush()
            new_metric_created.append(metric)
        except Exception as err:
            session.rollback()
            raise err
        finally:
            session.close()

        metric_ids_collected[metric] = id

        return id

    metric_ids_collected[metric] = obj.id

    return obj.id


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
        # payload is a list of dictionaries, each of them contains metrics as keys,
        # and 'age' key, which can have values like these examples: 45_49 or 50+
        for age_range_values in row.payload:
            if (
                age_range_values.get('age') is None
                or age_range_values['age'] not in age_metric_mapping
            ):
                # just ignore the values if 'age' is not what we want
                continue

            # checking only the required metrics
            for metric in age_metric_mapping[age_range_values['age']]:
                if metric not in age_range_values:
                    raise AttributeError(
                        f"Expected metric {metric} is missing int the DB data"
                    )

                metric_id = get_or_create_new_metric_id(metric, age_range_values['age'])

                # if for some reasons metric can't be created...
                if metric_id is None:
                    raise TypeError(
                        "Expected an integer as metric ID, but got 'None' "
                        f"for metric: {metric}. "
                        f"Fetched IDs: {json.loads(metric_ids_collected)}"
                    )

                enc_string = (
                    # The order here makes difference, as any other change to these
                    f"{str(row.release_id)}{str(row.area_id)}{str(metric_id)}"
                    f"{str(row.partition_id)}{row.date.isoformat()}"
                )
                hash = blake2s(digest_size=12)
                hash = blake2s(enc_string.encode('UTF-8'), key=RECORD_KEY, digest_size=12)
                hash_string = hash.hexdigest()
                new_hash_keys.add(hash_string)

                new_metric = TimeSeriesDataToDB(
                    hash_string,
                    row.release_id,
                    row.area_id,
                    metric_id,
                    row.partition_id,
                    row.date,
                    {'value': age_range_values[metric]}
                )
                new_metric_data.append(new_metric)

    logging.info(f"Number of new hash keys: {len(new_hash_keys)}")

    if new_metric_created:
        logging.info(f"These new metrics have been created: {new_metric_created}")
    else:
        logging.info(f"No new metrics have been created")

    return new_metric_data


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
    logging.info(
        f"Nested metrics function have been called with 'rawtimestamp': {rawtimestamp}"
    )

    if not rawtimestamp:
        return "No timestamp was provided for the nested metrics converter"

    # Getting the date -------------------------------------------------------------------
    ts = datetime.fromisoformat(rawtimestamp[:26])
    current_release_datestamp = date(year=ts.year, month=ts.month, day=ts.day)

    partition = f"{current_release_datestamp:%Y_%-m_%-d}"
    logging.info(f"The partition id (date related part): {partition}")

    # Set 2023-05-04 as cut off date used in the SQL query
    cutoff_date = date(year=2023, month=5, day=4)

    # Retrieving data (since the previous release) ---------------------------------------
    logging.info("Getting data from DB")
    values = from_sql(partition, cutoff_date)

    if values:
        logging.info(f"Retrieved {len(values)} rows from DB for nested metrics converter")
    else:
        logging.info("NO DATA COULD BE RETRIEVED FOR NESTED METRICS CONVERTER FUNCTION!")


    # Preparing the data for saving back to the DB ---------------------------------------
    new_list = convert_values(values)
    logging.info(f"Number of new nested metric rows to save/update: {len(new_list)}")


    # Saving data ------------------------------------------------------------------------
    to_sql(new_list)
    logging.info("All converted nested metrics have been saved to DB")

    return (
        "Process converting the nested metrics has compelted: "
        f"{current_release_datestamp}"
    )


# # This is not needed for prod, but useful for local development
# if __name__ == '__main__':
#     from sys import stdout

#     root = logging.getLogger()
#     root.setLevel(logging.DEBUG)
#     handler = logging.StreamHandler(stdout)
#     handler.setLevel(logging.DEBUG)
#     formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s')
#     handler.setFormatter(formatter)
#     root.addHandler(handler)

#     main("2023-11-09T16:39:14.123456")
