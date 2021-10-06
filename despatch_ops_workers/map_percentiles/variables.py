#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal:
try:
    from .queries import MSOA, NON_MSOA
except ImportError:
    from despatch_ops_workers.map_percentiles.queries import MSOA, NON_MSOA

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'PARAMETERS'
]


PARAMETERS = {
    'msoa': {
        'metric': 'newCasesBySpecimenDate',
        'attribute': 'rollingRate',
        'query': MSOA,
        'container': "downloads",
        'path': "maps/msoa_percentiles.json",
    },
    'nation': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': "maps/nation_percentiles.json",
    },
    'region': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': "maps/region_percentiles.json",
    },
    'utla': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': "maps/utla_percentiles.json",
    },
    'ltla': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': "maps/ltla_percentiles.json",
    }
}
