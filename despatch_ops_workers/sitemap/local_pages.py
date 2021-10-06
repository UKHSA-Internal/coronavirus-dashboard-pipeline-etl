#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import path
from urllib.parse import urlencode, quote
from lxml import etree

# 3rd party:
from pandas import read_csv

# Internal:
try:
    from __app__.utilities.settings import SITE_URL
except ImportError:
    from utilities.settings import SITE_URL

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_page_urls'
]


base_url = SITE_URL.strip("/")

curr_dir = path.split(path.abspath(__file__))[0]
lookup_table_path = path.join(curr_dir, "assets", "geoglist.csv")

locations = {
    "testing": [
        "nation"
    ],
    "healthcare": [
        "nation",
        "nhsRegion"
    ],
    "deaths": [
        "nation",
        "region"
    ],
    "cases": [
        "nation",
        "region",
        "utla",
        "ltla"
    ]
}


def to_xml_record(url: str, timestamp: str):
    root = etree.Element("url")

    loc = etree.SubElement(root, "loc")
    loc.text = url

    lastmod = etree.SubElement(root, "lastmod")
    lastmod.text = timestamp

    priority = etree.SubElement(root, "priority")
    priority.text = "0.80"

    changefreq = etree.SubElement(root, "changefreq")
    changefreq.text = "always"

    return etree.tostring(root, pretty_print=True).decode()


def get_urls(row, area_type, page_name, timestamp):
    url = urlencode({
        "areaType": area_type,
        "areaName": row['areaName']
    }, quote_via=quote)

    absolute_url = f"{base_url}/details/{page_name}?" + url
    # absolute_url = f"https://coronavirus-staging.data.gov.uk/details/{page_name}?" + url

    return to_xml_record(absolute_url, timestamp)


def get_page_urls(timestamp):
    lookup = read_csv(lookup_table_path, usecols=["areaType", "areaName"])
    results = list()
    lookup.areaType = lookup.areaType.str.lower()

    for page_name, area_types in locations.items():
        for area_type in area_types:
            page_urls = (
                lookup
                .loc[lookup.areaType == area_type, ["areaName"]]
                .apply(
                    get_urls,
                    area_type=area_type,
                    page_name=page_name,
                    timestamp=timestamp,
                    axis=1
                )
            )

            results.extend(page_urls)

    return str.join("", results)
