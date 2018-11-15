import os

import pandas
import pytest

from common.misc import intsafe_str
from conftest import TEST_DATA_DIR
from etl.extract.extract import ExtractStage
from etl.transform.standard_model.concept_schema import CONCEPT


def _get_output_and_expected(extract_config_path, expected_output_path):
    expected_output_path = 'file://' + expected_output_path
    es = ExtractStage([extract_config_path])
    df_out = es.run()
    example = df_out[extract_config_path][1]
    expected = es._source_file_to_df(expected_output_path)
    return example, expected


def _test_example_1():
    """
    Compare result of extracting test_study/extract_configs/example1.py against
    expected test_study/extract_outputs/example1_output.tsv
    """
    example, expected = _get_output_and_expected(
        os.path.join(
            TEST_DATA_DIR, 'test_study', 'extract_configs',
            'example1.py'
        ),
        os.path.join(
            TEST_DATA_DIR, 'test_study', 'extract_outputs',
            'example1_output.tsv'
        )
    )

    # accomodate file format deficiencies
    expected[CONCEPT.PARTICIPANT.MOTHER_ID] = expected[
        CONCEPT.PARTICIPANT.MOTHER_ID
    ].apply(intsafe_str)
    expected[CONCEPT.PARTICIPANT.FATHER_ID] = expected[
        CONCEPT.PARTICIPANT.FATHER_ID
    ].apply(intsafe_str)

    return example, expected


def _test_example_2():
    """
    Compare result of extracting test_study/extract_configs/example2.py against
    expected test_study/extract_outputs/example2_output.tsv
    """
    example, expected = _get_output_and_expected(
        os.path.join(
            TEST_DATA_DIR, 'test_study', 'extract_configs',
            'example2.py'
        ),
        os.path.join(
            TEST_DATA_DIR, 'test_study', 'extract_outputs',
            'example2_output.tsv'
        )
    )
    return example, expected


def test_extracts():
    for t in [_test_example_1, _test_example_2]:
        # get output and expected
        example, expected = t()
        # compare output with expected
        example = example[expected.columns]  # realign column order
        pandas.testing.assert_frame_equal(expected, example)
