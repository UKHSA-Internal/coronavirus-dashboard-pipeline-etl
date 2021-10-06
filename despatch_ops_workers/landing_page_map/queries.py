#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MAIN = """\
SELECT area_type AS "areaType",
       area_code AS "areaCode",
       (payload -> 'value')::FLOAT AS value
FROM covid19.time_series_p{date}_utla AS ts
      JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
      JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
WHERE metric = :metric
  AND (payload ->> 'value') NOTNULL
  AND date = DATE(:datestamp) - INTERVAL '5 days';
"""
