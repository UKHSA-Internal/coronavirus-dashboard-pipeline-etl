#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NamedTuple
import logging

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


class StorageEndpoint(NamedTuple):
    container: str
    path_template: str


storage_containers = [
    StorageEndpoint("apiv2cache", "{date}"),
    StorageEndpoint("ondemand", "easy_read/{date}")
]


def main(payload):
    logging.info(f"Purge storage activity triggered with payload: {payload}")

    purge_date = payload["date"]

    for endpoint in storage_containers:
        path = endpoint.path_template.format(date=purge_date)
        logging.info(f'Starting to deleting date for "{path}" in "{endpoint.container}".')

        with StorageClient(container=endpoint.container, path=path) as cli:
            blobs = [item for item in cli]
            container = cli.get_container()
            container.delete_blobs(*blobs)

            logging.info(f'Deleted {len(blobs)} blobs from "{path}" in "{endpoint.container}".')

    return f"DONE: {payload}"
