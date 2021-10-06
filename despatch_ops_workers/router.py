#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging

# 3rd party:

# Internal:
from .map_geojson import generate_geojson
from .map_vaccinations_geojson import generate_geojson as generate_vax_geojson
from .map_percentiles import generate_percentiles
from .archive_dates import generate_archive_dates
from .og_images import generate_og_images
from .sitemap import generate_sitemap
from .rate_scales import get_latest_scale_records, generate_scale_graph
from .landing_page_map import generate_landing_page_map
from .deploy_demographics import generate_demog_downloads
# from .temp_msoa_data import generate_msoa_data

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]


def main(payload):
    handlers = {
        "map_geojson": generate_geojson,
        "vax_map_geojson": generate_vax_geojson,
        "map_percentiles": generate_percentiles,
        "archive_dates": generate_archive_dates,
        "og_images": generate_og_images,
        "sitemap": generate_sitemap,
        "latest_scale_records": get_latest_scale_records,
        "scale_graphs": generate_scale_graph,
        "landing_page_map": generate_landing_page_map,
        "generate_demog_downloads": generate_demog_downloads,
        # "generate_msoa_data": generate_msoa_data
    }

    handler_name = payload["handler"]
    args = payload["payload"]

    logging.info(f"Activity '{handler_name}' triggered.")
    logging.info(f">> Payload: {args}")

    try:
        handler = handlers[handler_name]
        return handler(args)
    finally:
        logging.info(f">> Activity '{handler_name}' is done.")
