import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import logging
import os
import unittest

from db_etl_homepage_graphs.grapher import get_value_50_plus
from .test_data.db_data import VACCINATIONS_QUERY_50_PLUS


logging.basicConfig(level=logging.DEBUG)


class TestWaffleCharts(unittest.TestCase):
    def setUp(self) -> None:
        self.data = {
            "area_type": "nation",
            "area_code": "E92000001",
            "date": "2022-11-17",
            'payload': VACCINATIONS_QUERY_50_PLUS,
        }
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_get_value_50_plus(self):
        result = get_value_50_plus(self.data)
        
        self.assertEqual(result['vaccination_date'], 10866622)
        self.assertEqual(result['vaccination_date_percentage_dose'], 49)


if __name__ == '__main__':
    unittest.main()