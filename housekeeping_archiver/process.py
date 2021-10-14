#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
import tarfile
from typing import List, Iterator, Tuple, NoReturn
from tempfile import gettempdir, NamedTemporaryFile
from io import BytesIO
from pathlib import Path
from datetime import datetime
from orjson import dumps

# 3rd party:

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.housekeeping_orchestrator.dtypes import (
        ArtefactPayload, GenericPayload, MetaDataManifest
    )
except ImportError:
    from storage import StorageClient
    from housekeeping_orchestrator.dtypes import (
        ArtefactPayload, GenericPayload, MetaDataManifest
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ARCHIVE_CONTAINER = 'archives'


def get_blobs(container: str, file_paths: List[ArtefactPayload]) -> Iterator[Tuple[ArtefactPayload, BytesIO]]:
    """
    Generator to download artefact blobs, store them in a temp file
    and yields the file pointer.

    Parameters
    ----------
    container: str
        Storage container in which the artefact blobs are stored.

    file_paths: List[ArchivePayload]
        List of artefacts to download.

    Returns
    -------
    Iterator[Tuple[ArtefactPayload, BytesIO]]
        Tuple of artefact payload and file pointer.
    """
    logging.info("starting the generator to download artefact blobs")

    with StorageClient(container=container, path=file_paths[0]["from_path"]) as cli:
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


def upload_tarfile(archive_path: Path, storage_dir: str, filename: str, date: str, total_archived: int) -> NoReturn:
    """
    Uploads archived artefacts as a `tar.bz2` blob in the storage
    under an "Cool" tier.

    Parameters
    ----------
    archive_path: Path
        Path to the temp archive file.

    storage_dir: str
        Path to the directory in the `ARCHIVE_CONTAINER` where the archived
        artefacts are to be stored.

    filename: str
        Name by which to store the file in the storage.

    date: str
        Archive date - i.e. the date on which archived data were generated.

    total_archived: int
        Total number of artefacts included in the archive.

    Returns
    -------
    NoReturn
    """
    logging.info("uploading the Tar archive")

    storage_kws = dict(
        container=ARCHIVE_CONTAINER,
        path=f"{storage_dir}/{filename}",
        compressed=False,
        tier='Cool'
    )

    with StorageClient(**storage_kws) as cli, \
            open(archive_path, 'rb') as fp:
        cli.upload(fp.read())
        cli.client.set_blob_metadata({
            "date": date,
            "generated_on": datetime.utcnow().isoformat(),
            "total_artefacts": str(total_archived)
        })

    logging.info(f"Tar archive uploaded: {storage_kws}")

    return None


def main(payload: GenericPayload) -> GenericPayload:
    """
    Downloads artefact blobs, stores them in a Tar archive, and
    uploads the archive into the storage.

    Parameters
    ----------
    payload: GenericPayload
        Trigger data with artefacts (`ArtefactPayload`) as tasks.

    Returns
    -------
    GenericPayload
        Artefact that have been archived.
    """
    task_manifest = payload['manifest']
    logging.info(f"triggered with manifest: {task_manifest}")

    payload_content: List[ArtefactPayload] = payload["tasks"]

    # Archive is temporarily stored on disk.
    temp_dir = gettempdir()
    filename = f"{payload_content[0]['date']}.tar.bz2"
    archive_path = Path(temp_dir).joinpath(filename).resolve().absolute()

    manifest = list()
    archived = list()

    with tarfile.open(archive_path, mode='w:bz2') as archive_file:
        logging.info("generated tar file")

        for artefact, data in get_blobs(task_manifest['container'], payload_content):
            tar_info = archive_file.gettarinfo(fileobj=data)

            tar_info.path = artefact['filename']
            archive_file.addfile(tar_info, data)

            manifest.append(
                MetaDataManifest(
                    original_artefact=artefact,
                    archive_path=tar_info.path
                )
            )

            archived.append(artefact)

        # Storing manifest as a temp file
        with NamedTemporaryFile('w+b') as fp:
            fp.write(dumps(manifest))
            fp.seek(0)
            tar_info = archive_file.gettarinfo(fileobj=fp)
            tar_info.path = "/manifest.json"
            archive_file.addfile(tar_info, fp)

        logging.info("generated the manifest")

    logging.info("tar file composition is complete")

    upload_tarfile(
        archive_path=archive_path,
        storage_dir=task_manifest['archive_directory'],
        filename=filename,
        date=payload_content[0]['date'],
        total_archived=len(payload_content)
    )

    logging.info(f"completed for {payload['timestamp']}")

    response = GenericPayload(
        manifest=task_manifest,
        timestamp=payload['timestamp'],
        environment=payload['environment'],
        tasks=archived
    )

    return response
