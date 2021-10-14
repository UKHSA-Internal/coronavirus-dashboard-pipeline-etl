#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
import re
from typing import List
from collections import defaultdict
from datetime import datetime, timedelta

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.housekeeping_orchestrator.dtypes import (
        RetrieverPayload, ArchiverPayload, GenericPayload
    )
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import (
        RetrieverPayload, ArtefactPayload, GenericPayload
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]


def main(payload: RetrieverPayload) -> List[GenericPayload]:
    """
    Identifies artefacts that are candidates for being archived.

    Parameters
    ----------
    payload: Retriever Payload

    Returns
    -------
    List[GenericPayload]
        List of tasks.
    """
    task_manifest = payload['manifest']
    logging.info(f"triggered with manifest: {task_manifest}")

    timestamp = datetime.fromisoformat(payload['timestamp'])

    # Calculate offset based on manifest
    offset = timedelta(days=task_manifest['offset_days'])
    max_date = f"{timestamp - offset:%Y-%m-%d}"

    # Compile blob path pattern based on manifest
    pattern = re.compile(task_manifest['regex_pattern'], re.I)

    candidates = defaultdict(list)

    with StorageClient(task_manifest['container'], task_manifest['directory']) as cli:
        for blob in cli.list_blobs():
            path = pattern.search(blob["name"])

            try:
                content_type = blob['content_settings']['content_type']
            except KeyError:
                content_type = "application/octet-stream"

            if path is None:
                logging.info(f'Unmatched pattern: {blob["name"]}')
                continue

            # Parse and generated ISO formatted date stamp.
            artefact_date = datetime.strptime(path['date'], task_manifest['date_format'])
            formatted_artefact_date = f"{artefact_date}:%Y-%m-%d"

            # Ignore artefacts created after
            # the offset period.
            if formatted_artefact_date > max_date:
                continue

            parsed_data = path.groupdict()

            # Replace date with ISO-formatted date stamp.
            parsed_data['date'] = formatted_artefact_date

            blob_data = ArtefactPayload(**parsed_data, content_type=content_type)

            candidates[formatted_artefact_date].append(blob_data)

    artefacts = [
        GenericPayload(
            timestamp=payload['timestamp'],
            environment=payload['environment'],
            manifest=payload['manifest'],
            tasks=item
        ) for item in candidates.values()
    ]

    logging.info(f"done processing - length: {len(artefacts)}")

    return artefacts
