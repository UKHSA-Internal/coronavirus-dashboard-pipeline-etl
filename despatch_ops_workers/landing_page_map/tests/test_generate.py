import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent.parent
site.addsitedir(root_path)

import json
import logging
import pandas
import unittest
from unittest.mock import patch

from despatch_ops_workers.landing_page_map.generate import (
    generate_landing_page_map,
    plot_map,
)


logging.basicConfig(level=logging.DEBUG)


class TestLandingPageMap(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pandas.read_csv("test_data.csv")
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def get_geojson_replacement():
        with open("utla-ref-test.json", "r") as fh:
            return json.load(fh)

    def get_style_replacement():
        with open("style_v4-test.json", "r") as fh:
            return json.load(fh)

    def get_data_replacement(_):
        return pandas.read_csv("empty_dataframe.csv")

    def store_image_replacement(_):
        # not saving anything
        pass

    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_geojson",
        get_geojson_replacement
    )
    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_style",
        get_style_replacement
    )
    def test_plot_map_with_low_values(self):
        image = plot_map(self.data)

        self.assertGreater(len(image), 0)

    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_geojson",
        get_geojson_replacement
    )
    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_style",
        get_style_replacement
    )
    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_data",
        get_data_replacement
    )
    @patch(
        "despatch_ops_workers.landing_page_map.generate.store_image",
        store_image_replacement
    )
    def test_plot_map_with_no_values(self):
        payload = {"timestamp": "2023-01-31"}
        output = generate_landing_page_map(payload)

        assert output == (
            f"ERROR: landing page map at '{payload['timestamp']}' "
            "has not been generated"
        )


if __name__ == '__main__':
    unittest.main()