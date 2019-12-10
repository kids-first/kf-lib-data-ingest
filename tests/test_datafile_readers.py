import os

import pandas
import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.common.datafile_readers import *

excel_path = os.path.join(
    TEST_DATA_DIR, "test_study", "data", "unsimple_xlsx_1.xlsx"
)
tsv_path = os.path.join(
    TEST_DATA_DIR, "test_study", "data", "simple_headered_tsv_1.tsv"
)
excel_first_page = pandas.DataFrame(
    {"Nothing": ["to", "see", "here", ".", "Try", "the", "next", "page", "."]}
)
# fmt: off
tsv_page = pandas.DataFrame({
    'participant': ['PID001', 'PID002', 'PID003', 'PID004', 'PID005', 'PID006', 'PID007', 'PID008', 'PID009', 'PID010', 'PID011', 'PID012', 'PID013', 'PID014', 'PID015', 'PID016', 'PID017', 'PID018', 'PID019', 'PID020', 'PID021', 'PID022', 'PID023', 'PID024'],
    'mother': ['2', '', '', '5', '', '', '8', '', '', '11', '', '', '14', '', '', '17', '', '', '20', '', '', '23', '', ''],
    'father': ['3', '', '', '6', '', '', '9', '', '', '12', '', '', '15', '', '', '18', '', '', '21', '', '', '24', '', ''],
    'gender': ['F', '', '', 'M', '', '', 'M', '', '', 'M', '', '', 'M', '', '', 'F', '', '', 'M', '', '', 'M', '', ''],
    'consent': ['1', '1', '1', '2', '1', '2', '1', '1', '1', '1', '2', '1', '1', '2', '3', '3', '3', '2', '3', '3', '3', '3', '3', '3'],
    'age in hours': ['4', '435', '34', '4', '345', '34', '34', '43545', '5', '52', '25', '2', '23', '5', '235', '23', '53', '53', '5', '53', '5', '35', '35', '3'],
    'CLEFT_EGO': ['TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE'],
    'CLEFT_ID': ['FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'TRUE', 'TRUE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'FALSE'],
    'age in hours.1': ['4', '435', '34', '4', '34', '43545', '5', '52', '25', '2', '23', '5', '5', '4', '345', '34', '34', '43545', '23', '5', '5', '52', '25', '2'],
    'EXTRA_EARDRUM': ['FALSE', 'FALSE', 'FALSE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'FALSE', 'FALSE', 'FALSE', 'FALSE', 'FALSE', 'TRUE', 'TRUE', 'FALSE', 'TRUE', 'TRUE', 'TRUE', 'TRUE', 'FALSE', 'FALSE']
})
# fmt: on


def _file_reader_test(file_path, read_func, expected_contents):
    pandas.testing.assert_frame_equal(read_func(file_path), expected_contents)
    with open(file_path, "rb") as ef:
        pandas.testing.assert_frame_equal(read_func(ef), expected_contents)


def test_read_excel():
    _file_reader_test(excel_path, read_excel_file, excel_first_page)


def test_read_tsv():
    _file_reader_test(tsv_path, read_table_file, tsv_page)


def test_read_file():
    _file_reader_test(excel_path, read_file, excel_first_page)
    _file_reader_test(tsv_path, read_file, tsv_page)
