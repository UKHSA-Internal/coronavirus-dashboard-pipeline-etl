#!/usr/bin python3

PUBLISH_DATE_CALCULATION = """\
SELECT df.partition_id,
       df.area_id,
       df.area_type,
       df.area_code,
       mr.id,
       df.release_id,
       df.date,
       df.payload
FROM (
    SELECT today.partition_id,
           today.area_id,
           today.area_type,
           today.area_code,
           'newVirusTestsByPublishDate' AS metric,
           today.release_id,
           today.date,
           jsonb_build_object(
               'value',
               CASE
                   WHEN (today.value - yesterday.value)::INT < 0
                     THEN 0
                   ELSE (today.value - yesterday.value)::INT
               END
           ) AS payload
    FROM (
        SELECT area_id,
               partition_id,
               area_type,
               area_code,
               metric,
               MAX(ts.release_id) AS release_id,
               MAX(date) AS date,
               MAX((payload -> 'value')::NUMERIC) AS value
        FROM covid19.time_series AS ts
          JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
          JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
          JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
          JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
        WHERE metric = 'cumVirusTestsBySpecimenDate'
          AND area_type = ANY('{region,utla,ltla}'::VARCHAR[])
          AND partition_id = ANY((:today_partitions)::TEXT[])
          AND (payload ->> 'value') NOTNULL
          AND process_name = 'TESTING: MAIN'
        GROUP BY partition_id, area_id, area_type, area_code, metric
    ) AS today
    LEFT JOIN (
        SELECT area_id,
               partition_id,
               area_type,
               area_code,
               metric,
               MAX(ts.release_id) AS release_id,
               MAX(date) AS date,
               MAX((payload -> 'value')::NUMERIC) AS value
        FROM covid19.time_series AS ts
          JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
          JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
          JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
          JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
        WHERE metric = 'cumVirusTestsBySpecimenDate'
          AND area_type = ANY('{region,utla,ltla}'::VARCHAR[])
          AND partition_id = ANY((:yesterday_partitions)::TEXT[])
          AND (payload ->> 'value') NOTNULL
          AND process_name = 'TESTING: MAIN'
        GROUP BY partition_id, area_id, area_type, area_code, metric
    ) AS yesterday ON today.area_id = yesterday.area_id AND today.metric = yesterday.metric
) AS df
LEFT JOIN covid19.metric_reference AS mr ON mr.metric = df.metric
UNION
(
    SELECT partition_id,
           area_id,
           area_type,
           area_code,
           (SELECT mrr.id FROM covid19.metric_reference AS mrr WHERE metric = 'cumVirusTestsByPublishDate') AS id,
           ts.release_id AS release_id,
           date AS date,
           jsonb_build_object(
               'value',
               (payload ->> 'value')::NUMERIC::INT
           ) AS payload
    FROM covid19.time_series AS ts
      JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
      JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
      JOIN covid19.release_reference  AS rr ON rr.id = ts.release_id
      JOIN covid19.release_category   AS rc ON rc.release_id = rr.id
    WHERE metric = 'cumVirusTestsBySpecimenDate'
      AND area_type = ANY('{region,utla,ltla}'::VARCHAR[])
      AND partition_id = ANY((:today_partitions)::TEXT[])
      AND (payload ->> 'value') NOTNULL
      AND process_name = 'TESTING: MAIN'
      AND ts.date IN (
            SELECT MAX(date)
            FROM covid19.time_series AS ts
              JOIN covid19.metric_reference   AS mr ON mr.id = ts.metric_id
              JOIN covid19.area_reference     AS ar ON ar.id = ts.area_id
            WHERE metric = 'cumVirusTestsBySpecimenDate'
              AND area_type = ANY('{region,utla,ltla}'::VARCHAR[])
              AND partition_id = ANY((:today_partitions)::TEXT[])
        )
)\
"""
