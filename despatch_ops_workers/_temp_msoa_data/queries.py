#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


MAIN = """\
WITH d1 AS (
     SELECT area_code,
            JSONB_AGG(payload || jsonb_build_object('date', date)) AS value
     FROM covid19.time_series_p{date}_msoa AS ts
         JOIN covid19.area_reference    AS ar ON ts.area_id = ar.id
         JOIN covid19.release_reference AS rr ON rr.id = ts.release_id
         JOIN covid19.metric_reference  AS mr ON mr.id = ts.metric_id
     WHERE metric = 'newCasesBySpecimenDate'
     GROUP BY rr.timestamp, area_type, area_code, area_name
 )
SELECT 'MSOA' || '|' || ar.area_code AS id,
       ar.area_code                  AS "areaCode",
       area_type                     AS "areaType",
       area_name                     AS "areaName",
       jsonb_build_object(
               'newCasesBySpecimenDate',
               payload || jsonb_build_object('date', date)
           )                         AS latest,
       d1.value AS "newCasesBySpecimenDate",
       REPLACE(rr.timestamp::TEXT, ' ', 'T') || '5Z' AS release
FROM covid19.time_series_p{date}_msoa AS ts
    JOIN covid19.area_reference AS ar ON ts.area_id = ar.id
    JOIN covid19.release_reference AS rr ON rr.id = ts.release_id
    JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
    JOIN d1 ON d1.area_code = ar.area_code
WHERE metric = 'newCasesBySpecimenDate'
  AND date IN (SELECT MAX(date) FROM covid19.time_series_p{date}_msoa);\
"""
