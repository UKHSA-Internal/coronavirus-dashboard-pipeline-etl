#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
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
        ArtefactPayload, GenericPayload,  DisposerPayload
    )
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import (
        ArtefactPayload, GenericPayload, DisposerPayload
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def get_blobs(file_paths: List[ArtefactPayload]) -> Iterator[Tuple[ArtefactPayload, BytesIO]]:
    """
    Generator to download artefact blobs, store them in a temp file
    and yields the file pointer.

    Parameters
    ----------
    file_paths: List[ArchivePayload]
        List of artefacts to download.

    Returns
    -------
    Iterator[Tuple[ArtefactPayload, BytesIO]]
        Tuple of artefact payload and file pointer.
    """
    logging.info("starting the generator to download artefact blobs")

    with StorageClient(container="pipeline", path=file_paths[0]["from_path"]) as cli:
        container = cli.get_container()

        for artefact in file_paths:
            blob_obj = container.download_blob(artefact['from_path'])

            # Temporarily store downloaded blobs
            with NamedTemporaryFile(mode='w+b') as fp:
                blob_obj.readinto(fp)
                fp.seek(0)

                yield artefact, fp

        container.close()

    logging.info("all artefacts have been downloaded + processed")


def upload_tarfile(archive_path: Path, filename: str, date: str) -> NoReturn:
    """
    Uploads archived artefacts as a `tar.bz2` blob in the storage
    under an "Archive" tier.

    Parameters
    ----------
    archive_path: Path
        Path to the temp archive file.

    filename: str
        Name by which to store the file in the storage.

    date: str
        Archive date - i.e. the date on which archived data were generated.

    Returns
    -------
    NoReturn
    """
    logging.info("uploading the Tar archive")

    storage_kws = dict(
        container="pipeline",
        path=f"etl-archive/{filename}",
        compressed=False,
        tier='Archive'
    )

    with StorageClient(**storage_kws) as cli, \
            open(archive_path, 'rb') as fp:
        cli.upload(fp.read())
        cli.client.set_blob_metadata({
            "date": date,
            "generated_on": datetime.utcnow().isoformat()
        })

    logging.info(f"Tar archive uploaded: {storage_kws}")


def get_archive_path(artefact: ArtefactPayload) -> str:
    """
    Generates the archive path for the artefact based on its properties.

    Parameters
    ----------
    artefact: ArtefactPayload
        Artefact object whose archive path is to be generated.

    Returns
    -------
    str
        Archive path for the artefact.
    """
    if artefact["subcategory"] is None:
        return join(artefact["category"], artefact["filename"])

    return join(artefact["category"], artefact["subcategory"], artefact["filename"])


def main(payload: GenericPayload) -> DisposerPayload:
    """
    Downloads artefact blobs, stores them in a Tar archive, and
    uploads the archive into the storage.

    Parameters
    ----------
    payload: GenericPayload
        Trigger data with artefacts (`ArtefactPayload`) as content.

    Returns
    -------
    DisposerPayload
        Artefact that have been archived and are ready for removal.
    """
    logging.info(f"triggered for {payload['timestamp']}")

    payload_content: List[ArtefactPayload] = payload["content"]

    # Archive is temporarily stored on disk.
    temp_dir = gettempdir()
    filename = f"{payload_content[0]['date']}.tar.bz2"
    path = Path(temp_dir).joinpath(filename).resolve().absolute()

    manifest = list()
    archived = list()

    with tarfile.open(path, mode='w:bz2') as archive_file:
        logging.info("generated tar file")

        for artefact, data in get_blobs(payload_content):
            tar_info = archive_file.gettarinfo(fileobj=data)

            tar_info.path = get_archive_path(artefact)
            archive_file.addfile(tar_info, data)

            manifest.append({**artefact, 'archive_path': tar_info.path})
            archived.append(artefact['from_path'])

        # Storing manifest as a temp file
        with NamedTemporaryFile('w+b') as fp:
            fp.write(dumps(manifest))
            fp.seek(0)
            tar_info = archive_file.gettarinfo(fileobj=fp)
            tar_info.path = "/manifest.json"
            archive_file.addfile(tar_info, fp)

        logging.info("generated the manifest")

    logging.info("tar file composition is complete")

    upload_tarfile(path, filename=filename, date=payload_content[0]['date'])

    logging.info(f"completed for {payload['timestamp']}")

    return DisposerPayload(removables=archived, date=payload_content[0]['date'])
