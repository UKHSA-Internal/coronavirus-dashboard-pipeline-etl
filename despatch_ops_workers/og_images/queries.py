#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MAIN = """\
SELECT metric, date, (payload -> 'value')::FLOAT as value
FROM covid19.time_series_p{date}_other AS ts
JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
WHERE area_name = 'United Kingdom'
  AND metric = :metric
  AND (payload ->> 'value') NOTNULL
ORDER BY date DESC
LIMIT 1;
"""
