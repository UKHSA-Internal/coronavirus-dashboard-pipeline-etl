#!/usr/bin python3

STATS_QUERY = """\
INSERT INTO covid19.release_stats (release_id, record_count)
SELECT ts.release_id AS id, COUNT(*) AS counter
FROM covid19.time_series AS ts
WHERE ts.partition_id = ANY('{partitions}'::VARCHAR[])
GROUP BY ts.release_id
ON CONFLICT ( release_id ) DO
    UPDATE SET record_count = EXCLUDED.record_count;\
"""


PERMISSIONS_QUERY = """\
GRANT  USAGE                                ON SCHEMA covid19 TO   reader;
REVOKE CREATE                               ON SCHEMA covid19 FROM reader;
REVOKE TRUNCATE         ON ALL TABLES       IN SCHEMA covid19 FROM reader;
REVOKE UPDATE           ON ALL TABLES       IN SCHEMA covid19 FROM reader;
REVOKE DELETE           ON ALL TABLES       IN SCHEMA covid19 FROM reader;
REVOKE INSERT           ON ALL TABLES       IN SCHEMA covid19 FROM reader;
GRANT  SELECT           ON ALL TABLES       IN SCHEMA covid19 TO   reader;
GRANT  SELECT           ON ALL SEQUENCES    IN SCHEMA covid19 TO   reader;
REVOKE EXECUTE          ON ALL FUNCTIONS    IN SCHEMA covid19 FROM reader;
REVOKE TRIGGER          ON ALL TABLES       IN SCHEMA covid19 FROM reader;\
"""
