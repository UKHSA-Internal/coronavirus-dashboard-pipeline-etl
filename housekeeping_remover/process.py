#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging

# 3rd party:

# Internal: 
try:
    from __app__.storage import StorageClient
    from __app__.housekeeping_orchestrator.dtypes import RemoverPayload, GenericPayload
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import RemoverPayload, GenericPayload

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def main(payload: GenericPayload):
    payload_content: RemoverPayload = payload['content']
    logging.info(f"Triggered - total blobs to remove: {len(payload['content'])}")

    first_path = payload_content['removables'][0]
    with StorageClient(container="pipeline", path=first_path) as cli:
        container = cli.get_container()
        container.delete_blobs(*payload_content['removables'])

    logging.info(f"Done: {payload['timestamp']}")

    return f"DONE: {payload['timestamp']}"
