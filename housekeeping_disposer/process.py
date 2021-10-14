#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import List

# 3rd party:

# Internal: 
try:
    from __app__.storage import StorageClient
    from __app__.housekeeping_orchestrator.dtypes import (
        ArtefactPayload, GenericPayload, DisposerResponse
    )
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import (
        ArtefactPayload, GenericPayload, DisposerResponse
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def main(payload: GenericPayload) -> DisposerResponse:
    """
    Removes blobs from the storage.

    Parameters
    ----------
    payload: GenericPayload

    Returns
    -------
    DisposerResponse
        Message confirming that the process is done.
    """
    logging.info(f"triggered with manifest: {payload['manifest']}")
    logging.info(f"- total blobs to remove: {len(payload['tasks'])}")

    payload_content: List[ArtefactPayload] = payload['tasks']

    first_path = payload_content[0]['from_path']
    with StorageClient(container=payload['manifest']['container'], path=first_path) as cli:
        container = cli.get_container()

        for artefact in payload_content:
            container.delete_blob(artefact['from_path'])

        container.close()

    logging.info(f"done: {payload['timestamp']}")

    return DisposerResponse(total_processed=len(payload['tasks']))
