#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


NON_MSOA = """\
SELECT
    date,
    jsonb_build_object(
        'date', date,
        'code', area_code,
        'value', (payload ->> '{attr}')::FLOAT  -- value (JSON attribute)
    ) AS properties,
    jsonb_build_object(
        'type', geometry_type,
        'coordinates', coordinates
    ) AS geometry
FROM covid19.time_series_p{date}_{area_type} AS ts
LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
WHERE mr.metric = :metric  -- metric
  AND area_type = :area_type  -- area type
  AND (payload ->> '{attr}') NOTNULL  -- value (JSON attribute)
  AND date IN (
    SELECT DISTINCT date
    FROM covid19.time_series_p{date}_msoa
    WHERE date > (NOW() - INTERVAL '6 months')
  )
ORDER BY date DESC;\
"""


MSOA = """\
SELECT
    date,
    jsonb_build_object(
        'date', date,
        'code', area_code,
        'value', (payload -> '{attr}')  -- value (JSON attribute)
    ) AS properties,
    jsonb_build_object(
        'type', geometry_type,
        'coordinates', coordinates
    ) AS geometry
FROM covid19.time_series_p{date}_{area_type} AS ts
LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
WHERE mr.metric = :metric  -- metric
  AND (payload ->> '{attr}') NOTNULL  -- JSON attribute
  AND area_type = :area_type -- Needed for parameter compatibility
  AND date > (DATE(NOW()) - INTERVAL '6 months');\
"""