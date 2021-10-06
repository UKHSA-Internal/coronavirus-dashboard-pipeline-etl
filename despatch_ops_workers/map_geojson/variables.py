#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from dataclasses import dataclass

# 3rd party:

# Internal: 
try:
    from .queries import MSOA, NON_MSOA
except ImportError:
    from despatch_ops_workers.map_geojson.queries import MSOA, NON_MSOA

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'Device',
    'PARAMETERS'
]


@dataclass()
class Device:
    mobile = "MOBILE"
    desktop = "DESKTOP"


PARAMETERS = {
    'msoa': {
        'metric': 'newCasesBySpecimenDate',
        'attribute': 'rollingRate',
        'query': MSOA,
        'container': "downloads",
        'path': {
            Device.desktop: "maps/msoa_data_latest.geojson",
            Device.mobile: "maps/msoa_data_latest-mobile.geojson",
        }
    },
    'utla': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': {
            Device.desktop: "maps/utla_data_latest.geojson",
            Device.mobile: "maps/utla_data_latest-mobile.geojson",
        }
    },
    'ltla': {
        'metric': 'newCasesBySpecimenDateRollingRate',
        'attribute': 'value',
        'query': NON_MSOA,
        'container': "downloads",
        'path': {
            Device.desktop: "maps/ltla_data_latest.geojson",
            Device.mobile: "maps/ltla_data_latest-mobile.geojson",
        }
    }
}