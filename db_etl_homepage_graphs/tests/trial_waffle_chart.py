import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import logging
import os
import unittest
from unittest.mock import patch

from db_etl_homepage_graphs.grapher import get_vaccinations_75_plus


logging.basicConfig(level=logging.DEBUG)


# TODO: It's been used, but should be converted to a proper tool, or removed otherwise.
# It might be useful for local development.
class TestWaffleCharts(unittest.TestCase):
    """
    This started as a test (it mocks some functions),
    but it's a tool to manually generate waffle chart images now.
    """
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def file_upload_replacement(date, metric, csv, area_type=None, area_code=None):
        logging.debug(f" {date}, {metric}, {area_type}, {area_code}")

        if area_code is not None:
            downloads_dir = os.path.join(
                test_dir, "downloads", "homepage", date, metric, area_type
            )
            if not os.path.exists(downloads_dir):
                os.makedirs(downloads_dir)

            path = os.path.join(downloads_dir, f"{area_code}_75_plus_thumbnail.svg")

            with open(path, "w") as fh:
                fh.write(csv)


    @patch('db_etl_homepage_graphs.grapher.upload_file', file_upload_replacement)
    def test_get_vaccinations_75_plus(self):

        get_vaccinations_75_plus("2023-05-18")


if __name__ == '__main__':
    unittest.main()
