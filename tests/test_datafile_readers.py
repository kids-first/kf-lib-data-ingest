import os

import pandas
import pytest

from common import datafile_readers
from conftest import TEST_DATA_DIR

excel_path = os.path.join(
    TEST_DATA_DIR, 'test_study', 'data', 'unsimple_xlsx_1.xlsx'
)
first_page = pandas.DataFrame(
    {'Nothing': ['to', 'see', 'here', '.', 'Try', 'the', 'next', 'page', '.']}
)


def test_read_excel_path():
    pandas.testing.assert_frame_equal(
        datafile_readers.read_excel_file(excel_path),
        first_page
    )


def test_read_excel_fp():
    with open(excel_path, "rb") as ep:
        pandas.testing.assert_frame_equal(
            datafile_readers.read_excel_file(ep),
            first_page
        )
