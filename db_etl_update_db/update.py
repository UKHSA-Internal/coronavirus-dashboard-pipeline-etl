#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime

# 3rd party:
from sqlalchemy import text

# Internal:
try:
    from __app__.db_tables.covid19 import Session
except ImportError:
    from db_tables.covid19 import Session

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


STATS_QUERY = """\
INSERT INTO covid19.release_stats (release_id, record_count)
SELECT ts.release_id AS id, COUNT(*) AS counter
FROM covid19.time_series AS ts
WHERE 
      ts.release_id IN (
          SELECT rr.id 
          FROM covid19.release_reference AS rr
          WHERE rr.timestamp::DATE = '{datestamp}'::DATE
      )
  AND ts.partition_id = ANY('{partitions}'::VARCHAR[])
GROUP BY ts.release_id
ON CONFLICT ( release_id ) DO
    UPDATE SET record_count = EXCLUDED.record_count;\
"""


PERMISSIONS_QUERY = """\
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
GRANT  USAGE                                                ON SCHEMA covid19 TO   reader;
REVOKE CREATE                                               ON SCHEMA covid19 FROM reader;
REVOKE TRUNCATE                         ON ALL TABLES       IN SCHEMA covid19 FROM reader;
REVOKE UPDATE, DELETE, INSERT           ON ALL TABLES       IN SCHEMA covid19 FROM reader;
GRANT  SELECT                           ON ALL TABLES       IN SCHEMA covid19 TO   reader;
GRANT  SELECT                           ON ALL SEQUENCES    IN SCHEMA covid19 TO   reader;
REVOKE EXECUTE                          ON ALL FUNCTIONS    IN SCHEMA covid19 FROM reader;
REVOKE TRIGGER                          ON ALL TABLES       IN SCHEMA covid19 FROM reader;
COMMIT;\
"""


def update_permissions():
    session = Session()
    connection = session.connection()
    try:
        connection.execute(text(PERMISSIONS_QUERY))
        session.flush()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def get_partition_ids(date, category):
    date = datetime.strptime(date, "%Y-%m-%d")
    partition_names = [
        "other",
        "utla",
        "ltla",
        "nhstrust",
        "msoa"
    ]

    if category == "msoa":
        partition_names = ["msoa"]

    partitions = [
        f"{date:%Y_%-m_%-d}|{partition}"
        for partition in partition_names
    ]

    return partitions


def update_stats(date, category):
    session = Session()
    connection = session.connection()
    try:
        connection.execute(
            text(STATS_QUERY.format(
                datestamp=date,
                partitions=f'{{{str.join(",", get_partition_ids(date, category))}}}'
            ))
        )
        session.flush()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def main(payload):
    category = payload.get("category")

    update_permissions()
    update_stats(payload['date'], category)

    return f"DONE - {payload['date']}"


if __name__ == "__main__":
    main({"date": "2021-03-28"})
