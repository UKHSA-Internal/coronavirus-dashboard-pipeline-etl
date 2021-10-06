#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


NON_MSOA = """\
SELECT *
FROM (
    SELECT
        'complete' AS date,
        MIN((payload -> '{attr}')::FLOAT) AS min,  -- JSON attribute
        percentile_disc(.25) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS first,  -- JSON attribute
        percentile_disc(.50) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS second,  -- JSON attribute
        percentile_disc(.75) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS third,  -- JSON attribute
        MAX((payload -> '{attr}')::FLOAT) AS max   -- JSON attribute
    FROM covid19.time_series_p{date}_{area_type} AS ts
    LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
    LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
    LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
    WHERE mr.metric = :metric  -- Metric
      AND area_type = :area_type  -- Area type
      AND (payload ->> '{attr}') NOTNULL  -- JSON attribute
    UNION (
        SELECT
            date::TEXT,
            MIN((payload -> '{attr}')::FLOAT) AS min,  -- JSON attribute
            percentile_disc(.25) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS first,  -- JSON attribute
            percentile_disc(.50) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS second,  -- JSON attribute
            percentile_disc(.75) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS third,  -- JSON attribute
            MAX((payload -> '{attr}')::FLOAT) AS max   -- JSON attribute
        FROM covid19.time_series_p{date}_{area_type} AS ts
        LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
        LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
        LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
        WHERE mr.metric = :metric  -- Metric
          AND area_type = :area_type  -- Area type
          AND (payload ->> '{attr}') NOTNULL  -- JSON attribute
          AND date > (DATE(NOW()) - INTERVAL '6 months')
        GROUP BY date
    )
) AS dt
WHERE date IN (SELECT DISTINCT date::TEXT FROM covid19.time_series_p{date}_msoa)
   OR date = 'complete' 
ORDER BY date;\
"""


MSOA = """\
SELECT
    'complete' AS date,
    MIN((payload -> '{attr}')::FLOAT) AS min,  -- JSON attribute
    percentile_disc(.25) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS first,  -- JSON attribute
    percentile_disc(.50) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS second,  -- JSON attribute
    percentile_disc(.75) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS third,  -- JSON attribute
    MAX((payload -> '{attr}')::FLOAT) AS max   -- JSON attribute
FROM covid19.time_series_p{date}_{area_type} AS ts
LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
WHERE mr.metric = :metric  -- Metric
  AND area_type = :area_type  -- Area type
  AND (payload ->> '{attr}') NOTNULL  -- JSON attribute
UNION (
    SELECT
        date::TEXT,
        MIN((payload -> '{attr}')::FLOAT) AS min,  -- JSON attribute
        percentile_disc(.25) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS first,  -- JSON attribute
        percentile_disc(.50) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS second,  -- JSON attribute
        percentile_disc(.75) WITHIN GROUP (ORDER BY (payload -> '{attr}')::FLOAT) AS third,  -- JSON attribute
        MAX((payload -> '{attr}')::FLOAT) AS max   -- JSON attribute
    FROM covid19.time_series_p{date}_{area_type} AS ts
    LEFT JOIN covid19.area_reference    AS ar  ON ar.id = ts.area_id
    LEFT JOIN covid19.metric_reference  AS mr  ON mr.id = metric_id
    LEFT JOIN covid19.geo_data          AS geo ON ar.id = geo.area_id
    WHERE mr.metric = :metric  -- Metric
      AND area_type = :area_type  -- Area type
      AND (payload ->> '{attr}') NOTNULL  -- JSON attribute
      AND date > (DATE(NOW()) - INTERVAL '6 months')
    GROUP BY date
)
ORDER BY date;\
"""
