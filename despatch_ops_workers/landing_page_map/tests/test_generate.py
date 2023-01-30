import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent.parent
site.addsitedir(root_path)

import logging
import pandas
import unittest

from despatch_ops_workers.landing_page_map.generate import plot_map


logging.basicConfig(level=logging.DEBUG)


class TestLandingPageMap(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pandas.read_csv("test_data.csv")
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_plot_map(self):
        image = plot_map(self.data)

        self.assertGreater(len(image), 0)


if __name__ == '__main__':
    unittest.main()