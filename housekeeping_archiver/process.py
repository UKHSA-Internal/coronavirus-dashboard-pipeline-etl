#!/usr/bin python3


# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import List, Iterator, Tuple, NoReturn
from tempfile import gettempdir, NamedTemporaryFile
from io import BytesIO
from os.path import join
from pathlib import Path
from datetime import datetime
from orjson import dumps
import tarfile

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.housekeeping_orchestrator.dtypes import (
        ArchiverPayload, GenericPayload,  RemoverPayload
    )
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import (
        ArchiverPayload, GenericPayload, RemoverPayload
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def get_blobs(file_paths: List[ArchiverPayload]) -> Iterator[Tuple[ArchiverPayload, BytesIO]]:
    with StorageClient(container="pipeline", path=file_paths[0]["from_path"]) as cli:
        container = cli.get_container()

        for item in file_paths:
            blob_obj = container.download_blob(item['from_path'])

            with NamedTemporaryFile(mode='w+b') as fp:
                blob_obj.readinto(fp)
                fp.seek(0)
                yield item, fp

        container.close()


def upload_tarfile(archive_path: Path, filename: str, date: str) -> NoReturn:
    timestamp = datetime.utcnow().isoformat()
    storage_kws = dict(
        container="pipeline",
        path=f"etl-archive/{filename}",
        compressed=False,
        tier='Archive'
    )

    with StorageClient(**storage_kws) as cli:
        with open(archive_path, 'rb') as fp:
            cli.upload(fp.read())
            cli.client.set_blob_metadata({"date": date, "generated_on": timestamp})


def main(payload: GenericPayload) -> RemoverPayload:
    payload_content: List[ArchiverPayload] = payload["content"]
    temp_dir = gettempdir()
    filename = f"{payload_content[0]['date']}.tar.bz2"
    path = Path(temp_dir).joinpath(filename).resolve().absolute()

    manifest = list()
    archived = list()

    with tarfile.open(path, mode='w:bz2') as archive_file:
        for archive_obj, data in get_blobs(payload_content):
            tar_info = archive_file.gettarinfo(fileobj=data)

            if archive_obj["subcategory"] is None:
                archive_path = join(
                    archive_obj["category"],
                    archive_obj["filename"]
                )
            else:
                archive_path = join(
                    archive_obj["category"],
                    archive_obj["subcategory"],
                    archive_obj["filename"]
                )

            tar_info.path = archive_path
            archive_file.addfile(tar_info, data)

            manifest.append({
                **archive_obj,
                'archive_path': archive_path
            })

            archived.append(archive_obj['from_path'])

        with NamedTemporaryFile('w+b') as fp:
            fp.write(dumps(manifest))
            fp.seek(0)
            tar_info = archive_file.gettarinfo(fileobj=fp)
            tar_info.path = "/manifest.json"
            archive_file.addfile(tar_info, fp)

    upload_tarfile(path, filename=filename, date=payload_content[0]['date'])

    return RemoverPayload(removables=archived, date=payload_content[0]['date'])
