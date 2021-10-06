#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import tarfile
from tempfile import NamedTemporaryFile, gettempdir
from typing import Union, Iterable, AnyStr, IO
from datetime import datetime
from pathlib import Path

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient, StandardBlobTier, StorageStreamDownloader
except ImportError:
    from storage import StorageClient, StandardBlobTier, StorageStreamDownloader

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


TempDir = Path(gettempdir()).resolve()


class ArchiveStorage:
    def __init__(self, container):
        kws = dict(
            container=container,
            content_type="application/octet-stream",
            cache_control="no-cache, max-age=0, must-revalidate",
            compressed=True,
            tier='Archive'
        )

        self.client = StorageClient(**kws)
        self.container = self.client.get_container()
        self.content_settings = getattr(self.client, '_content_settings')

    def upload(self, path: str, data: Union[Iterable[AnyStr], IO[AnyStr]]):
        self.container.upload_blob(
            data=data,
            name=path,
            content_settings=self.content_settings,
            overwrite=True,
            standard_blob_tier=StandardBlobTier.Archive,
            timeout=60,
            max_concurrency=10
        )

    def download(self, path: str) -> StorageStreamDownloader:
        return self.container.download_blob(path)

    def ls_of(self, prefix):
        return self.container.walk_blobs(name_starts_with=prefix)

    def __enter__(self) -> 'ArchiveStorage':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.close()


def find_candidates(storage: ArchiveStorage, max_date: datetime, prefix):
    date = "2020-12-01"
    payload_dir = TempDir.joinpath(date)
    payload_dir.mkdir(exist_ok=True)

    for file_data in storage.ls_of(f"{prefix}/{date}/"):
        if file_data["name"].endswith(".tar.gz"):
            continue

        # print(file_data)
        # print(file_data["name"], file_data["last_modified"])
        file_name = Path(file_data["name"]).name
        filepath = payload_dir.joinpath(file_name)

        with open(filepath, "wb") as fp:
            storage.download(file_data["name"]).readinto(fp)

        yield filepath


def archive(paths, tar_archive):

    for index, path in enumerate(paths):
        if index > 10:
            return

        tar_archive.add(path, arcname=path.name)


def process(storage: ArchiveStorage):
    pass


def main():
    container = "pipeline"
    prefixes = [
        "daily_chunks/main"
    ]

    max_date = datetime(year=2020, month=12, day=2)

    with ArchiveStorage(container) as client, \
            tarfile.open("test.tar.gz", mode="w:gz") as tar_archive:
        paths = find_candidates(client, max_date, prefixes[0])
        archive(paths, tar_archive)


if __name__ == "__main__":
    main()
