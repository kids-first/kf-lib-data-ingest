import os

import pandas
import pytest

from conftest import TEST_DATA_DIR
from kf_lib_data_ingest.common.misc import intsafe_str
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
    CONCEPT
)

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
        )
    }
}


def test_extracts():
    for study_dir, study_configs in expected_results.items():
        extract_configs = list(study_configs.keys())
        es = ExtractStage(os.path.join(study_dir, 'output'), extract_configs)
        df_out = es.run()
        recycled_output = es._read_output()

        for config in extract_configs:
            extracted = df_out[config][1]
            expected = es._source_file_to_df(
                "file://" + study_configs[config]
            )

            # test for same columns (might be out of order)
            assert sorted(expected.columns) == sorted(extracted.columns)
            expected = expected[extracted.columns]

            # account for datatype mismatches induced by loading the expected
            # result directly from a file
            for col in expected.columns:
                if expected[col].dtype != extracted[col].dtype:
                    expected[col] = expected[col].apply(intsafe_str)

            # test for expected equivalence
            pandas.testing.assert_frame_equal(extracted, expected)

            # test serialize/deserialize equivalence
            pandas.testing.assert_frame_equal(
                extracted, recycled_output[config][1]
            )
