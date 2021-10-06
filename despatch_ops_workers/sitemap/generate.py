#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import path
from datetime import datetime

# 3rd party:
from jinja2 import FileSystemLoader, Environment

# Internal:
try:
    from __app__.storage import StorageClient
    from .local_pages import get_page_urls
except ImportError:
    from storage import StorageClient
    from despatch_ops_workers.sitemap.local_pages import get_page_urls

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_sitemap'
]


ABOUT_DATA_PATH = "assets/modals/about.md"
ACCESSIBILITY_DATA_PATH = "assets/modals/accessibility.md"

SITEMAP_PATH = "assets/supplements/sitemap.xml"

SITEMAP_KWS = dict(
    container="publicdata",
    content_type="application/xml",
    cache_control="no-cache, max-age=0",
    content_language=None,
    compressed=False
)

curr_dir = path.split(path.abspath(__file__))[0]
file_loader = FileSystemLoader(path.join(curr_dir, "templates"))
env = Environment(loader=file_loader)


def get_modal_timestamp(filepath):
    with StorageClient("publicdata", filepath) as client:
        for blob in client.list_blobs():
            return blob['last_modified'].strftime("%Y-%m-%dT%H:%M:%S+00:00")


def store_sitemap(data):
    with StorageClient(path=SITEMAP_PATH, **SITEMAP_KWS) as client:
        client.upload(data)


def generate_sitemap(payload):
    timestamp = datetime.fromisoformat(payload["timestamp"])
    formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    template = env.get_template("sitemap.xml")
    page_urls = get_page_urls(formatted_timestamp)

    sitemap = template.render(
        timestamp=formatted_timestamp,
        accessibility_timestamp=get_modal_timestamp(ACCESSIBILITY_DATA_PATH),
        about_timestamp=get_modal_timestamp(ABOUT_DATA_PATH),
        sub_pages=page_urls
    )

    store_sitemap(sitemap)

    return f"DONE: Sitemaps {payload['timestamp']}"


if __name__ == '__main__':
    generate_sitemap("")
