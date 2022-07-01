#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 
try:
    from .dtypes import Manifest, ProcessMode
except ImportError:
    from housekeeping_orchestrator.dtypes import Manifest, ProcessMode

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'housekeeping_tasks'
]


housekeeping_tasks = (
    Manifest(
        label='ETL chunks',
        container='pipeline',
        directory='etl_chunks',
        regex_pattern=r"^(?P<from_path>etl_chunks/(?P<filename>.+(?P<date>\d{4}-\d{2}-\d{2}).+\.ft))$",
        date_format="%Y-%m-%d",
        offset_days=5,
        archive_directory='etl_chunks',
        mode=ProcessMode.ARCHIVE_AND_DISPOSE
    ),
    Manifest(
        label='ETL disposables',
        container='pipeline',
        directory='etl',
        regex_pattern=r"^(?P<from_path>etl/(?P<filename>.+(?P<date>\d{4}-\d{2}-\d{2}).+\.ft))$",
        date_format="%Y-%m-%d",
        offset_days=5,
        archive_directory=None,
        mode=ProcessMode.DISPOSE_ONLY
    ),
    Manifest(
        label='Rate scales',
        container='publicdata',
        directory='assets/frontpage/scales',
        regex_pattern=r"^(?P<from_path>assets/frontpage/scales/(?P<filename>.*(?P<date>\d{4}-\d{2}-\d{2}).+))$",
        date_format="%Y-%m-%d",
        offset_days=10,
        archive_directory=None,
        mode=ProcessMode.DISPOSE_ONLY
    ),
    Manifest(
        label='Summary thumbnails',
        container='downloads',
        directory='homepage',
        regex_pattern=r"^(?P<from_path>homepage/(?P<filename>(?P<date>\d{4}-\d{2}-\d{2})/.+))$",
        date_format="%Y-%m-%d",
        offset_days=10,
        archive_directory='summary_thumbnails',
        mode=ProcessMode.ARCHIVE_AND_DISPOSE
    ),
    Manifest(
        label='Open Graph images',
        container='downloads',
        directory='og-images',
        regex_pattern=r"^(?P<from_path>og-images/(?P<filename>og-[a-z]+_(?P<date>\d{4}\d{2}\d{2}).png))$",
        date_format="%Y%m%d",
        offset_days=10,
        archive_directory='og_images',
        mode=ProcessMode.ARCHIVE_AND_DISPOSE
    ),
    Manifest(
        label='Easy-Read documents',
        container='ondemand',
        directory='easy_read',
        regex_pattern=r"^(?P<from_path>easy_read/(?P<filename>(?P<date>\d{4}-\d{2}-\d{2})/.+))$",
        date_format="%Y-%m-%d",
        offset_days=10,
        archive_directory='easy_read',
        mode=ProcessMode.ARCHIVE_AND_DISPOSE
    ),
    Manifest(
        label='Raw pipeline data',
        container='rawdbdata',
        directory='',
        regex_pattern=r"^(?P<from_path>(?P<date>\d{4}-\d{2}-\d{2})/(?P<filename>.+))$",
        date_format="%Y-%m-%d",
        offset_days=10,
        archive_directory='raw_db_data',
        mode=ProcessMode.ARCHIVE_AND_DISPOSE
    ),
)
