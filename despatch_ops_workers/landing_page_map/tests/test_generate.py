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
from os import environ, getenv


logging.basicConfig(level=logging.DEBUG)


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


def choropleth_mapbox_replacement(*args, **kwargs):
    # Using self.data in the test 4 items are expected in color_discrete_map
    # This eventual error will be caught in the test function
    assert len(kwargs['color_discrete_map']) == 4

    return None


class TestLandingPageMap(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pandas.read_csv("test_data.csv")
        # the original DB_URL will be replaced - just in case it's not set
        # or set to a incorrect value
        self.db_url = getenv('DB_URL')
        # this can be any valid string - not used but needed for initialisation
        environ['DB_URL'] = "postgresql://user:@localhost:5432/db"

        return super().setUp()

    def tearDown(self) -> None:
        if self.db_url:
            environ['DB_URL'] = self.db_url

        return super().tearDown()

    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_geojson",
        get_geojson_replacement
    )
    @patch(
        "despatch_ops_workers.landing_page_map.generate.get_style",
        get_style_replacement
    )
    def test_plot_map_with_low_values(self):
        """
        Original colour_scale_binning list can raise KeyError if the values don't cover
        the whole range of it.
        This test makes sure the KeyError doesn't occur in that case.
        """
        from despatch_ops_workers.landing_page_map.generate import (
            plot_map,
        )
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
        "plotly.express.choropleth_mapbox",
        choropleth_mapbox_replacement
    )
    def test_colour_scale_binning_length(self):
        """
        As too long colour_scale_binning list can raise KeyError it is reduced.
        This test makes sure the list was reduced to correct number of elements.
        """
        from despatch_ops_workers.landing_page_map.generate import (
            plot_map,
        )
        try:
            plot_map(self.data)
        except AssertionError as err:
            raise err
        # The AttributeError will always raise, as the plot_map function lacks some data
        # at this point the function can be terminated (other error will be raised)
        except AttributeError as err:
            self.assertRaises(AttributeError)

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
        from despatch_ops_workers.landing_page_map.generate import (
            generate_landing_page_map,
        )
        payload = {"timestamp": "2023-01-31"}
        output = generate_landing_page_map(payload)

        assert output == (
            f"ERROR: landing page map at '{payload['timestamp']}' "
            "has not been generated"
        )


if __name__ == '__main__':
    unittest.main()