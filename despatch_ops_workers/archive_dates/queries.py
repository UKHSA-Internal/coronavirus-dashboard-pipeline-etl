#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MAIN = """\
SELECT timestamp::DATE::TEXT AS date, timestamp::TEXT AS timestamp
FROM covid19.release_reference AS rr
LEFT JOIN covid19.release_category AS rc ON rr.id = rc.release_id 
WHERE rr.released IS TRUE
  AND rc.process_name = :process_name  -- Process name (MAIN, MSOA, ...)
ORDER BY rr.timestamp DESC;\
"""
