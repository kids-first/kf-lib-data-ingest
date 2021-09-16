import os

import pandas
import pytest
import requests_mock
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.misc import convert_to_downcasted_str
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.extract_config import ExtractConfig
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.extract.operations import column_map
from kf_lib_data_ingest.etl.extract.utils import Extractor
from numpy import NaN

from conftest import TEST_DATA_DIR

TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, "data.csv")

study_1 = os.path.join(TEST_DATA_DIR, "test_study")
study_subdirs = {
    study_1: {"config": "extract_configs", "output": "extract_outputs"}
}
expected_results = {
    study_1: {
        "simple_tsv_example1.py": "simple_tsv_example1_output.tsv",
        "simple_tsv_example2.py": "simple_tsv_example2_output.tsv",
        "unsimple_xlsx_example1.py": "unsimple_xlsx_example1_output.tsv",
        "split_rows_tsv_example1.py": "split_rows_tsv_example1_output.tsv",
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
        lambda x: convert_to_downcasted_str(x, replace_na=True, na="")
    )
    B = B.applymap(
        lambda x: convert_to_downcasted_str(x, replace_na=True, na="")
    )

    for col in A.columns:
        try:
            A[col] = A[col].astype(float)
            B[col] = B[col].astype(float)
        except Exception:
            pass

    return (
        A.sort_values(list(A.columns)).sort_index().reset_index(),
        B.sort_values(list(B.columns)).sort_index().reset_index(),
    )


def test_empty_read():
    """complain if we read an empty DF"""
    es = ExtractStage("", os.path.join(study_1, "extract_configs"))
    es.extract_configs[0].do_after_read = lambda x: pandas.DataFrame()
    with pytest.raises(ConfigValidationError):
        es.run()


def test_extracts():
    for study_dir, study_configs in expected_results.items():
        extract_configs = list(study_configs.keys())
        es = ExtractStage(
            os.path.join(study_dir, "output"),
            os.path.join(study_dir, study_subdirs[study_dir]["config"]),
        )
        df_out = es.run()
        recycled_output = es.read_output()
        for config in extract_configs:
            extracted = df_out[os.path.basename(config)]
            expected = es._source_file_to_df(
                "file://"
                + os.path.join(
                    study_dir,
                    study_subdirs[study_dir]["output"],
                    study_configs[config],
                )
            )

            expected.set_index("index", inplace=True)
            expected.index = expected.index.astype(int)
            expected.index.name = None

            # test serialize/deserialize equivalence
            A, B = rectify_cols_and_datatypes(
                extracted, recycled_output[os.path.basename(config)]
            )
            pandas.testing.assert_frame_equal(A, B)

            # test for expected equivalence
            A, B = rectify_cols_and_datatypes(expected, extracted)
            pandas.testing.assert_frame_equal(A, B)


def test_extractor_bad_inputs():
    """
    Test Extractor.extract with bad inputs
    """
    # Test bad inputs
    extractor = Extractor()
    # Not a DataFrame
    with pytest.raises(TypeError) as e:
        extractor.extract("foo", "/path")
    # Not a valid extract cfg path
    with pytest.raises(TypeError) as e:
        extractor.extract(pandas.DataFrame({"a": [1]}), 1)
    # An empty DataFrame
    with pytest.raises(Exception) as e:
        extractor.extract(pandas.DataFrame(), "/path")
        assert "cannot be empty" in e
    # An extract config validation error
    transform_module = os.path.join(study_1, "transform_module.py")
    with pytest.raises(ConfigValidationError) as e:
        extractor.extract(
            pandas.DataFrame({"a": [1]}), ExtractConfig(transform_module)
        )
    # The do_after_read produces an empty DataFrame
    es = ExtractStage("", os.path.join(study_1, "extract_configs"))
    ec = es.extract_configs[0]
    df = pandas.DataFrame({"a": [i for i in range(2)]})
    ec.do_after_read = lambda x: pandas.DataFrame()
    with pytest.raises(ConfigValidationError):
        Extractor().extract(df, ec, apply_after_read_func=True)


@requests_mock.Mocker(kw="mock")
def test_bad_file_types(**kwargs):
    es = ExtractStage("", "")
    mock = kwargs["mock"]

    # no type
    filename = os.path.basename(TEST_FILE_PATH)
    url = f"http://localhost:1234/{filename}/download"
    with open(TEST_FILE_PATH, "rb") as tf:
        mock.get(url, content=tf.read(), status_code=200)
    with pytest.raises(ConfigValidationError) as e:
        es._source_file_to_df(url)
    assert "Could not determine appropriate reader" in str(e.value)

    # unknown type
    with pytest.raises(ConfigValidationError) as e:
        es._source_file_to_df(
            "file://" + os.path.join(TEST_DATA_DIR, "yaml_schema.yml")
        )
    assert "Could not determine appropriate reader" in str(e.value)

    # good type but error in load function
    with pytest.raises(ConfigValidationError) as e:
        es._source_file_to_df(
            "file://" + os.path.join(TEST_DATA_DIR, "concept_graph.json")
        )
    assert "Could not determine appropriate reader" not in str(e.value)


def test_no_load_return():
    f = os.path.join(
        study_1, "extract_outputs", "simple_tsv_example1_output.tsv"
    )
    es = ExtractStage("", "")
    with pytest.raises(ConfigValidationError):
        es._source_file_to_df("file://" + f, read_func=lambda x: None)


def test_visibility():
    es = ExtractStage("", "")

    a = [True, "True", False, "False", None, NaN]
    clean_a = [True, True, False, False, None, None]
    b = [False, False, True, True, None, None]

    df_a = pandas.DataFrame({CONCEPT.FAMILY.HIDDEN: a})
    es.extractor._obvert_visibility(df_a)

    assert CONCEPT.FAMILY.VISIBLE in df_a
    assert CONCEPT.FAMILY.HIDDEN in df_a
    assert df_a[CONCEPT.FAMILY.HIDDEN].equals(pandas.Series(clean_a))
    assert df_a[CONCEPT.FAMILY.VISIBLE].equals(pandas.Series(b))

    df_a = pandas.DataFrame({CONCEPT.FAMILY.VISIBLE: a})
    es.extractor._obvert_visibility(df_a)

    assert CONCEPT.FAMILY.VISIBLE in df_a
    assert CONCEPT.FAMILY.HIDDEN in df_a
    assert df_a[CONCEPT.FAMILY.HIDDEN].equals(pandas.Series(b))
    assert df_a[CONCEPT.FAMILY.VISIBLE].equals(pandas.Series(clean_a))

    df_a = pandas.DataFrame(
        {CONCEPT.FAMILY.HIDDEN: a, CONCEPT.FAMILY.VISIBLE: a}
    )
    with pytest.raises(AssertionError):
        es.extractor._obvert_visibility(df_a)


def test_operation_length_errors():
    es = ExtractStage("", "")
    df = pandas.DataFrame({"A": [1, 2], "B": [1, 2]})

    # test too long result not in sublist
    op = [[column_map(in_col="A", out_col="NAME", m=lambda x: x.append(x))]]
    es.extractor._chain_operations(df, op)
    with pytest.raises(ConfigValidationError) as e:
        es.extractor._chain_operations(df, op[0])
    assert "nested operation sublists" in str(e.value)

    # test result not a multiple
    op = [lambda x: pandas.Series([1])]
    with pytest.raises(ConfigValidationError) as e:
        es.extractor._chain_operations(df, op)
    assert "not a multiple" in str(e.value)


def test_operation_skipping():
    es = ExtractStage("", "")
    df = pandas.DataFrame({"A": [1, 2], "B": [1, 2]})

    op = [
        column_map(in_col="C", out_col="NAME", m=lambda x: x, optional=True),
        column_map(in_col="B", out_col="NAME", m=lambda x: x),
    ]
    es.extractor._chain_operations(df, op)
