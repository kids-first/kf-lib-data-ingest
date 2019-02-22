import os

import pandas
import pytest
import requests_mock

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.misc import numeric_to_str
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.extract.extract import ExtractStage

TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, 'data.csv')

study_1 = os.path.join(TEST_DATA_DIR, 'test_study')
expected_results = {
    study_1: {
        os.path.join(study_1, 'extract_configs', 'simple_tsv_example1.py'):
            os.path.join(
                study_1, 'extract_outputs', 'simple_tsv_example1_output.tsv'
        ),
        os.path.join(study_1, 'extract_configs', 'simple_tsv_example2.py'):
            os.path.join(
                study_1, 'extract_outputs', 'simple_tsv_example2_output.tsv'
        ),
        os.path.join(study_1, 'extract_configs', 'unsimple_xlsx_example1.py'):
            os.path.join(
                study_1, 'extract_outputs', 'unsimple_xlsx_example1_output.tsv'
        ),
        os.path.join(study_1, 'extract_configs', 'split_rows_tsv_example1.py'):
            os.path.join(
                study_1,
                'extract_outputs', 'split_rows_tsv_example1_output.tsv'
        )
    }
}


def rectify_cols_and_datatypes(A, B):
    A = A.copy()
    B = B.copy()

    # test for same columns (might be out of order)
    assert sorted(A.columns) == sorted(B.columns)

    B = B[A.columns]

    # account for datatype mismatches induced by loading the expected
    # result directly from a file
    A = A.applymap(
        lambda x: numeric_to_str(x, replace_na=True, na='')
    )
    B = B.applymap(
        lambda x: numeric_to_str(x, replace_na=True, na='')
    )

    for col in A.columns:
        try:
            A[col] = A[col].astype(float)
            B[col] = B[col].astype(float)
        except Exception:
            pass

    return (
        A.sort_values(list(A.columns)).sort_index().reset_index(),
        B.sort_values(list(B.columns)).sort_index().reset_index()
    )


def test_extracts():
    for study_dir, study_configs in expected_results.items():
        extract_configs = list(study_configs.keys())
        es = ExtractStage(os.path.join(study_dir, 'output'), extract_configs)
        df_out, _, _ = es.run()
        recycled_output = es.read_output()

        for config in extract_configs:
            extracted = df_out[config][1]
            expected = es._source_file_to_df(
                'file://' + study_configs[config]
            )

            expected.set_index('index', inplace=True)
            expected.index = expected.index.astype(int)
            expected.index.name = None

            # test serialize/deserialize equivalence
            A, B = rectify_cols_and_datatypes(
                extracted, recycled_output[config][1]
            )
            pandas.testing.assert_frame_equal(A, B)

            # test for expected equivalence
            A, B = rectify_cols_and_datatypes(expected, extracted)
            pandas.testing.assert_frame_equal(A, B)


@requests_mock.Mocker(kw='mock')
def test_bad_file_types(**kwargs):
    es = ExtractStage(None, None)
    mock = kwargs['mock']

    # no type
    filename = os.path.basename(TEST_FILE_PATH)
    url = f'http://localhost:1234/{filename}/download'
    with open(TEST_FILE_PATH, "rb") as tf:
        mock.get(url, content=tf.read(), status_code=200)
    with pytest.raises(ConfigValidationError):
        try:
            es._source_file_to_df(url)
        except ConfigValidationError as e:
            assert "Could not determine appropriate loader" in str(e)
            raise

    # unknown type
    with pytest.raises(ConfigValidationError):
        try:
            es._source_file_to_df(
                'file://' +
                os.path.join(TEST_DATA_DIR, 'yaml_schema.yml')
            )
        except ConfigValidationError as e:
            assert "Could not determine appropriate loader" in str(e)
            raise

    # good type but error in load function
    with pytest.raises(ConfigValidationError):
        try:
            es._source_file_to_df(
                'file://' +
                os.path.join(TEST_DATA_DIR, 'concept_graph.json')
            )
        except ConfigValidationError as e:
            assert "Could not determine appropriate loader" not in str(e)
            raise


def test_no_load_return():
    f = os.path.join(
        study_1, 'extract_outputs', 'simple_tsv_example1_output.tsv'
    )
    es = ExtractStage(None, None)
    with pytest.raises(ConfigValidationError):
        es._source_file_to_df(
            'file://' + f,
            load_func=lambda x: None
        )
