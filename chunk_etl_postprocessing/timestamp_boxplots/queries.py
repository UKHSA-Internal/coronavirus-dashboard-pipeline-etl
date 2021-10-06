#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CATEGORY_TIMESTAMPS = """\
SELECT timestamp
FROM covid19.release_reference     AS rr
LEFT JOIN covid19.release_category AS rc ON rc.release_id = rr.id
WHERE process_name = :category 
  AND timestamp::DATE < (:timestamp)::DATE \
"""

