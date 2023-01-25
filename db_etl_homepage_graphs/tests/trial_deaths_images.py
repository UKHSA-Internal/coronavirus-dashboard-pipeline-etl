import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import logging
import os
import unittest
from unittest.mock import patch

from db_etl_homepage_graphs.grapher import get_timeseries


logging.basicConfig(level=logging.DEBUG)


# TODO: It's been used, but should be converted to a proper tool, or removed otherwise.
# It might be useful for local development.
class TestDeathsImages(unittest.TestCase):
    """
    This started as a test (it mocks some functions),
    but it's a tool to manually generate images now.
    """
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def store_data_replacement(date, metric, csv, area_type=None, area_code=None):
        logging.debug(f" {date}, {metric}, {area_type}, {area_code}")

        if area_code is not None:
            print("area_code is not None - exiting")
            return

        downloads_dir = f"download/homepage/{date}"
        filename = f"thumbnail_{metric}.svg"

        if not os.path.exists(downloads_dir):
            os.makedirs(downloads_dir)

        path = os.path.join(downloads_dir, filename)

        with open(path, "w") as fh:
            fh.write(csv)


    @patch('db_etl_homepage_graphs.grapher.store_data', store_data_replacement)
    def test_get_timeseries(self):

        get_timeseries("2023-01-25", "newDailyNsoDeathsByDeathDate")


if __name__ == '__main__':
    unittest.main()
