import site
import pathlib

test_dir = pathlib.Path(__file__).resolve().parent
root_path = test_dir.parent.parent
site.addsitedir(root_path)

import unittest
from pandas import read_csv

from db_etl_upload.uploader import  trim_sides


class TestLandingPageMap(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    def test_trim_sides(self):
        test_data = read_csv('test_data-trim_sides.csv')

        output_df = trim_sides(test_data)

        print(f"df size: {output_df.shape}")
        assert output_df.shape == (14, 9)


if __name__ == '__main__':
    unittest.main()