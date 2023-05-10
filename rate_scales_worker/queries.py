#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


RATES = """\
SELECT area_type,
       area_code,
       (payload -> :attr)::FLOAT AS value
FROM covid19.time_series_p{date}_{area_type} AS ts
    JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
    JOIN covid19.area_reference   AS ar ON ar.id = ts.area_id
WHERE metric = :metric  -- Metric
  AND (payload ->> :attr) NOTNULL
  AND area_type = :area_type
  AND date IN (
      SELECT MAX(date)
      FROM covid19.time_series_p{date}_{area_type} AS ts
          JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
          JOIN covid19.area_reference   AS ar ON ar.id = ts.area_id
      WHERE metric = :metric
        AND area_type = :area_type
    );\
"""
