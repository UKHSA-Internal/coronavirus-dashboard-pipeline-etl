#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple, List

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.specimen_date_demographics.constants import NEXT_DEPLOYMENT_PATH as SPC_DATE_PATH
    from __app__.publish_date_demographics.constants import NEXT_DEPLOYMENT_PATH as PUB_DATE_PATH
except ImportError:
    from storage import StorageClient
    from specimen_date_demographics.constants import NEXT_DEPLOYMENT_PATH as SPC_DATE_PATH
    from publish_date_demographics.constants import NEXT_DEPLOYMENT_PATH as PUB_DATE_PATH

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_demog_downloads'
]

SRC_CONTAINER = "pipeline"
TRG_CONTAINER = "downloads"


class CopyPayload(NamedTuple):
    source_container: str
    source: str
    target_container: str
    target: str


copy_paths: List[CopyPayload] = [
    CopyPayload(
        source_container=SRC_CONTAINER,
        source=PUB_DATE_PATH.format(filename="stacked"),
        target_container=TRG_CONTAINER,
        target="demographic/cases/publishDate_ageDemographic-stacked.csv"
    ),
    CopyPayload(
        source_container=SRC_CONTAINER,
        source=PUB_DATE_PATH.format(filename="unstacked"),
        target_container=TRG_CONTAINER,
        target="demographic/cases/publishDate_ageDemographic-unstacked.csv"
    ),
    CopyPayload(
        source_container=SRC_CONTAINER,
        source=SPC_DATE_PATH.format(filename="stacked"),
        target_container=TRG_CONTAINER,
        target="demographic/cases/specimenDate_ageDemographic-stacked.csv"
    ),
    CopyPayload(
        source_container=SRC_CONTAINER,
        source=SPC_DATE_PATH.format(filename="unstacked"),
        target_container=TRG_CONTAINER,
        target="demographic/cases/specimenDate_ageDemographic-unstacked.csv"
    )
]


def copy_dateset(payload: CopyPayload):
    filename = payload.target.split("/")[-1]

    with StorageClient(
            container=payload.source_container,
            path=payload.source,
            compressed=False,
            cache_control="max-age: 60, must-revalidate",
            content_language="en-GB",
            content_disposition=f'attachment; filename="{filename}"',
            content_type="text/csv; charset=utf-8"
    ) as cli:
        cli.copy_blob(payload.target_container, payload.target)


def generate_demog_downloads(payload):
    for item in copy_paths:
        copy_dateset(item)

    return f"DONE: copy demogs dataset {payload['timestamp']}"
