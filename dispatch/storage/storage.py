#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       22 Jun 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from typing import Union, NoReturn
from gzip import compress

# 3rd party:
from azure.storage.blob import BlobClient, BlobType, ContentSettings

# Internal: 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "upload_file",
    "download_file",
    "StorageClient"
]


STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8"
DEFAULT_CACHE_CONTROL = "no-cache, max-age=0, stale-while-revalidate=300"
CONTENT_LANGUAGE = 'en-GB'


def download_file(container: str, path: str) -> bytes:
    client = BlobClient.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name=container,
        blob_name=path
    )
    data = client.download_blob()

    return data.readall()


def upload_file(container: str, path: str, data: Union[str, bytes],
                content_type: str, cache: str = "no-store") -> NoReturn:

    client = BlobClient.from_connection_string(
        conn_str=STORAGE_CONNECTION_STRING,
        container_name=container,
        blob_name=path,
    )

    client.upload_blob(
        data,
        blob_type=BlobType.BlockBlob,
        content_settings=ContentSettings(
            content_type=content_type,
            cache_control=cache
        ),
        overwrite=True
    )


class StorageClient:
    """
    Azure Storage client.

    Parameters
    ----------
    container: str
        Storage container.

    path: str
        Path to the blob (excluding ``container``).

    connection_string: str
        Connection string (credentials) to access the storage unit. If not supplied,
        will look for ``DeploymentBlobStorage`` in environment variables.

    content_type: str
        Sets the MIME type of the blob via the ``Content-Type`` header - used
        for uploads only.

        Default: ``application/json; charset=utf-8``

    cache_control: str
        Sets caching rules for the blob via the ``Cache-Control`` header - used
        for uploads only.

        Default: ``no-cache, max-age=0, stale-while-revalidate=300``

    compressed: bool
        If ``True``, will compress the data using `GZip` at maximum level and
        sets ``Content-Encoding`` header for the blob to ``gzip``. If ``False``,
        it will upload the data without any compression.

        Default: ``True``

    content_language: str
        Sets the language of the data via the ``Content-Language`` header - used
        for uploads only.

        Default: ``en-GB``
    """

    def __init__(self, container: str, path: str,
                 connection_string: str = STORAGE_CONNECTION_STRING,
                 content_type: Union[str, None] = DEFAULT_CONTENT_TYPE,
                 cache_control: str = DEFAULT_CACHE_CONTROL, compressed: bool = True,
                 content_disposition: Union[str, None] = None,
                 content_language=CONTENT_LANGUAGE):
        self.path = path
        self.compressed = compressed

        self._content_settings: ContentSettings = ContentSettings(
            content_type=content_type,
            cache_control=cache_control,
            content_encoding="gzip" if self.compressed else None,
            content_language=content_language,
            content_disposition=content_disposition
        )

        self.client: BlobClient = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container,
            path=path,
            blob_name=path,
        )

    def __enter__(self) -> 'StorageClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> NoReturn:
        self.client.close()

    def upload(self, data: Union[str, bytes]) -> NoReturn:
        """
        Uploads blob data to the storage.

        Parameters
        ----------
        data: Union[str, bytes]
            Data to be uploaded to the storage.

        Returns
        -------
        NoReturn
        """
        if self.compressed:
            prepped_data = compress(data.encode() if isinstance(data, str) else data)
        else:
            prepped_data = data

        self.client.upload_blob(
            data=prepped_data,
            blob_type=BlobType.BlockBlob,
            content_settings=self._content_settings,
            overwrite=True
        )
        logging.info(f"Uploaded file '{self.path}'")