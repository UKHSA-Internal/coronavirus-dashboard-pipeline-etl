import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import logging
import os
import unittest

from db_etl_homepage_graphs.grapher import get_value_75_plus
from db_etl_homepage_graphs.tests.test_data.db_data import VACCINATIONS_QUERY_PLUS


logging.basicConfig(level=logging.DEBUG)


class TestWaffleCharts(unittest.TestCase):
    def setUp(self) -> None:
        self.data = {
            "area_type": "nation",
            "area_code": "E92000001",
            "date": "2022-11-17",
            'payload': VACCINATIONS_QUERY_PLUS,
        }
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_get_value_75_plus(self):
        result = get_value_75_plus(self.data)
        print(result)
        
        self.assertEqual(result['vaccination_date'], 4178999)
        self.assertEqual(result['vaccination_date_percentage_dose'], 48)


if __name__ == '__main__':
    unittest.main()