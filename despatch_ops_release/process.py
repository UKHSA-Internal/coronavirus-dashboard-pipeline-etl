#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:

# 3rd party:

# Internal: 
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "ReleaseTimestamps",
    "main"
]


ReleaseTimestamps = [
    {
        "process": lambda ts: ts.split("T")[0],
        "path": "info/seriesDate",
        "container": "pipeline"
    },
    {
        "process": lambda ts: f"{ts}Z",
        "path": "assets/dispatch/website_timestamp",
        "container": "publicdata"
    },
    {
        "process": lambda ts: f"{ts}5Z",
        "path": "info/latest_published",
        "container": "pipeline",
    }
]


UPLOAD_KWS = dict(
    content_type="text/plain; charset=utf-8",
    cache="no-cache, max-age=0",
    compressed=False
)


def main(payload):
    kws = {
        **UPLOAD_KWS,
        **payload
    }
    value = kws.pop("value")

    with StorageClient(**kws) as client:
        client.upload(value)

    return f"DONE: {payload}"
