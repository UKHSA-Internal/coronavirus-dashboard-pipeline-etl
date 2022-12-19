# import pathlib
# import site

# test_dir = pathlib.Path(__file__).resolve().parent
# root_path = test_dir.parent
# print(f"root path => {root_path}")
# site.addsitedir(root_path)

import logging
from collections import namedtuple
from datetime import datetime, timedelta
from hashlib import blake2s
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert


try:
    from __app__.db_tables.covid19 import MainData, MetricReference, Session
except ImportError:
    from db_tables.covid19 import MainData, MetricReference, Session


__all__ = [
    'main'
]


METRIC_IDS_QUERY="""\
SELECT metric, id
FROM covid19.metric_reference
WHERE metric = ANY('{metrics_string}');\
"""
MOST_RECENT_DATA_DATE = """\
SELECT date
FROM (
        SELECT metric, date
        FROM covid19.time_series_p{partition}_other AS tsother
                JOIN covid19.metric_reference AS mr ON mr.id = metric_id
        WHERE metric = 'vaccinationsAgeDemographics'
        AND date > ( DATE(NOW()) - INTERVAL '13 days')
        UNION
        (
            SELECT metric, date
            FROM covid19.time_series_p{partition}_utla AS tsutla
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
        WHERE metric = 'vaccinationsAgeDemographics'
        AND date > ( DATE(NOW()) - INTERVAL '13 days')
        )
        UNION
        (
            SELECT metric, date
            FROM covid19.time_series_p{partition}_ltla AS tsltla
                    JOIN covid19.metric_reference AS mr ON mr.id = metric_id
        WHERE metric = 'vaccinationsAgeDemographics'
        AND date > ( DATE(NOW()) - INTERVAL '13 days')
        )
    ) AS tsltla
ORDER BY date DESC
LIMIT 1;
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
# The nested metrics that have to be converted to regular ones
METRICS_TO_CONVERT = [
    "cumPeopleVaccinatedAutumn22ByVaccinationDate",
    "cumVaccinationAutumn22UptakeByVaccinationDatePercentage",
]


TimeSeriesData = namedtuple(
    'TimeSeriesData',
    ['partition_id', 'release_id', 'area_id', 'date', 'payload']
)
TimeSeriesDataToDB = namedtuple(
    'TimeSeriesDataToDB',
    ['hash', 'release_id', 'area_id', 'metric_id', 'partition_id', 'date', 'payload']
)
age_range_suffix = ['50+']
metric_ids_collected = dict()
new_metric_created = list()


def to_sql(data):
    session = Session()
    connection = session.connection()

    try:
        for chunk in data:
            insert_statement = insert(MainData.__table__).values(chunk)
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


def from_sql(partition, date):
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


def get_or_create_new_metric_id(metric):
    # if the metric ID is already known, then use it
    if metric_ids_collected.get(metric, None):
        return metric_ids_collected[metric]

    session = Session()
    connection = session.connection()

    obj = None

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


def convert_values(data):
    new_metric_data = []
    new_hash_keys = set()

    for row in data:
        for values in row.payload:
            if values.get('age', None) is None or values['age'] not in age_range_suffix:
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
    logging.info(f"These new metrics have been created: {new_metric_created}\n")

    return new_metric_data


def get_partition():
    """ This downloads 'latest_published' file from the blob storage
        and returns its content, which is the timestamp of the latest release
    """
    from storage import StorageClient


    path = 'info/latest_published'

    with StorageClient(container="pipeline", path=path) as cli:
        date = cli.download().read()

    return date.decode("UTF-8")


def simple_db_query(query):
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
def main(input):
    """ The function to create new metrics (if necessary) listed in METRICS_TO_CONVERT
        constant. If they already exist, it will only retrieve their IDs.
        It collects relevant data, transforms it and then saves it (the new metrics
        will be concatenation of the nested metric name and the age range).
        Nothing will be removed.
    """
    # Getting date -----------------------------------------------------------------------
    latest_published_date_string = get_partition().replace('Z', '')

    # for some reasons fromisoformat() expects milliseconds as 6 digits
    if len(latest_published_date_string) > 26:
        latest_published_as_dt = datetime.fromisoformat(latest_published_date_string[:26])

    ts = datetime.strptime(latest_published_as_dt.isoformat(), "%Y-%m-%dT%H:%M:%S.%f")
    partition = f"{ts:%Y_%-m_%-d}"
    print(f"\nData will be retrieved from '{partition}' partition")


    # Fetching the last day when data was processed/saved by ETL pipeline ----------------
    query = MOST_RECENT_DATA_DATE.format(partition=partition)
    last_date = simple_db_query(query)[0][0]
    print(f"The date of the last data saved in DB: {last_date} / {type(last_date)}")


    # Get dates of the 2 latest releases -------------------------------------------------
    query = (
        "SELECT DISTINCT(timestamp::DATE) "
        "FROM covid19.release_reference "
        "ORDER BY timestamp DESC "
        "LIMIT 2;"
    )
    previous_release_date = simple_db_query(query)[1][0]
    print(previous_release_date, {type(previous_release_date)})


    # Retrieving data (since the previous release) ---------------------------------------
    values = from_sql(partition, previous_release_date - timedelta(days=1))
    print(f"Retrieved {len(values)} rows from the DB")


    # Preparing the data for saving back to the DB ---------------------------------------
    new_list = convert_values(values)
    print(f"Number of new data after the conversion: {len(new_list)}")


    # Saving data ------------------------------------------------------------------------
    to_sql(new_list)

    return f"DONE: {input['timestamp']}"


if __name__ == '__main__':
    main({"timestamp": datetime.fromisoformat("2022-12-08T15:15:15.123456")})
