import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import logging
import os
import unittest
from unittest.mock import patch

from db_etl_homepage_graphs.grapher import get_vaccinations, get_vaccinations_50_plus


logging.basicConfig(level=logging.DEBUG)


class TestWaffleCharts(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def replacement_store(date, metric, csv, area_type=None, area_code=None):
        logging.debug(f" {date}, {metric}, {area_type}, {area_code}")

        if area_code is not None and area_type == 'nation':
            downloads_dir = os.path.join(test_dir, "downloads", date, metric, area_type)
            if not os.path.exists(downloads_dir):
                os.makedirs(downloads_dir)

            path = os.path.join(downloads_dir, f"{area_code}_thumbnail.svg")

            with open(path, "w") as fh:
                fh.write(csv)

    @patch('db_etl_homepage_graphs.grapher.store_data', replacement_store)
    def test_get_vaccinations(self):
        logging.debug("STARTED")


        get_vaccinations("2022-11-17")

    @patch('db_etl_homepage_graphs.grapher.store_data_50_plus', replacement_store)
    def test_get_vaccinations_50_plus(self):

        get_vaccinations_50_plus("2022-11-17")


if __name__ == '__main__':
    unittest.main()
