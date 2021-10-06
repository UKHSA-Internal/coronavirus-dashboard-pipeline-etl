#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv, makedirs
from os.path import split as split_path
from typing import Union, NoReturn

# 3rd party:
from azure.storage.blob import BlobClient, BlobType, ContentSettings, StandardBlobTier

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'TestOutput',
    'MainOutput'
]


CONTAINER_NAME = getenv("StorageContainerName")
STORAGE_CONNECTION_STRING = getenv("DeploymentBlobStorage")
RAW_DATA_CONTAINER = "rawdbdata"
DEBUG = getenv("DEBUG", False)


class TestOutput:
    def __init__(self, path):
        self.path = path

        if DEBUG:
            self.path = f"test/v2/{path}"

    def set(self, data: Union[str, bytes], content_type: str = "application/json",
            cache: str = "no-store") -> NoReturn:
        mode = "w" if isinstance(data, str) else "wb"

        dir_path, _ = split_path(self.path)
        makedirs(dir_path, exist_ok=True)

        with open(self.path, mode=mode) as output_file:
            print(data, file=output_file)


class MainOutput(TestOutput):
    def __init__(self, *args, chunks=False, **kwargs):
        super().__init__(*args, **kwargs)

        self.chunks = chunks

        # if not chunks:
        self.client = BlobClient.from_connection_string(
            conn_str=STORAGE_CONNECTION_STRING,
            container_name=CONTAINER_NAME,
            blob_name=self.path,
            **kwargs
        )

    def set(self, data: Union[str, bytes], content_type: str = "application/json",
            cache: str = "no-store") -> NoReturn:
        self.client.upload_blob(
            data,
            blob_type=BlobType.BlockBlob,
            content_settings=ContentSettings(
                content_type=content_type,
                cache_control=cache
            ),
            overwrite=True,
            standard_blob_tier=StandardBlobTier.Cool
        )
