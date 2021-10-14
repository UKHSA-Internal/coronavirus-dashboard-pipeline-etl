#!/usr/bin python3

from typing import TypedDict, Union, List
from enum import IntEnum


__all__ = [
    "ArtefactPayload",
    "RetrieverPayload",
    "GenericPayload",
    "Manifest",
    "ProcessMode",
    "MetaDataManifest",
    'DisposerResponse'
]


class ProcessMode(IntEnum):
    ARCHIVE_AND_DISPOSE = 0
    ARCHIVE_ONLY = 1
    DISPOSE_ONLY = 2


class Manifest(TypedDict):
    label: str
    container: str
    directory: str
    regex_pattern: str
    date_format: str
    offset_days: int
    archive_directory: Union[str, None]
    mode: ProcessMode


class ArtefactPayload(TypedDict):
    date: str
    filename: str
    from_path: str
    content_type: str


class MetaDataManifest(TypedDict):
    original_artefact: ArtefactPayload
    archive_path: str


class RetrieverPayload(TypedDict):
    timestamp: str
    environment: str
    manifest: Manifest


class DisposerResponse(TypedDict):
    total_processed: int


class GenericPayload(TypedDict):
    timestamp: str
    environment: str
    tasks: Union[List[ArtefactPayload], RetrieverPayload]
    manifest: Manifest
