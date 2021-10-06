#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       08 Sep 2021
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


MAIN_QUERY = """\
WITH data AS (
    SELECT MAX(release_id) AS release_id,
           area_id,
           metric,
           ts.date AS "date",
           RANK() OVER (
               PARTITION BY metric, area_id
               ORDER BY date DESC
           ) AS row_num,
           MAX((payload ->> 'value'))::NUMERIC AS value
    FROM covid19.time_series_p{partition_date}_other AS ts
      JOIN covid19.release_reference AS rr ON rr.id = ts.release_id
      FULL OUTER JOIN covid19.metric_reference AS mr ON mr.id = ts.metric_id
      FULL OUTER JOIN covid19.area_reference AS ar ON ar.id = ts.area_id
    WHERE area_type IN ('overview', 'nation')
      AND metric = ANY((:metrics)::VARCHAR[])
    GROUP BY area_id, metric, ts.date
)
SELECT release_id, area_id, metric, date, value
FROM data
WHERE row_num = 1;\
"""


OUTPUT_DATA = """\
SELECT area_name,
       metric,
       date,
       value
FROM covid19.private_report AS pr
  JOIN covid19.area_reference AS ar ON ar.id = pr.area_id
WHERE slug_id = :slug_id;\
"""
