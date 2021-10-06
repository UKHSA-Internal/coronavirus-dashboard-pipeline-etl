#!/usr/bin python3


# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple, List

# 3rd party:
from pandas import DataFrame

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'ArchivePayload',
    'RawDataPayload',
    'PopulationData',
    'ETLResponse'
]


class PopulationData(NamedTuple):
    general: DataFrame
    ageSex5YearBreakdown: DataFrame
    ageSexBroadBreakdown: DataFrame


class ETLResponse(NamedTuple):
    path: str
    area_type: str
    area_code: str
    date: str
    environment: str


class ArchivePayload(NamedTuple):
    results: List[ETLResponse]
    original_path: str
    timestamp: str
    environment: str


class RawDataPayload(NamedTuple):
    data_path: str
    timestamp: str
    environment: str
