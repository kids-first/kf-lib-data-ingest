import pandas as pd
import pytest

from conftest import TRANSFORM_MODULE_PATH
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule,
)
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage


@pytest.fixture(scope="function")
def df_dict():
    """
    Mock input to GuidedTransformStage._standard_to_target
    """
    dfs = {
        "family": pd.DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.FAMILY.UNIQUE_KEY: ["f1", "f2", "f3"],
            }
        ),
        "participant": pd.DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.DIAGNOSIS.NAME: ["cold", "cold", None],
                CONCEPT.PARTICIPANT.GENDER: ["Female", "Male", "Female"],
            }
        ),
        "diagnosis": pd.DataFrame(
            {CONCEPT.DIAGNOSIS.UNIQUE_KEY: ["p1-cold", "p2-cold", None]}
        ),
        "biospecimen": pd.DataFrame(
            {
                CONCEPT.PARTICIPANT.UNIQUE_KEY: ["p1", "p2", "p2"],
                CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN.ANALYTE: ["dna", "rna", "dna"],
            }
        ),
        "sequencing_experiment": pd.DataFrame(
            {
                CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.BIOSPECIMEN_GROUP.UNIQUE_KEY: ["b1", "b2", "b3"],
                CONCEPT.SEQUENCING.LIBRARY_NAME: ["lib1", "lib2", "lib3"],
            }
        ),
    }

    df = outer_merge(
        dfs["family"],
        dfs["participant"],
        on=CONCEPT.PARTICIPANT.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    df = outer_merge(
        df,
        dfs["biospecimen"],
        on=CONCEPT.PARTICIPANT.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    df = outer_merge(
        df,
        dfs["sequencing_experiment"],
        on=CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
        with_merge_detail_dfs=False,
    )
    return {"participant": df, DEFAULT_KEY: df}


@pytest.fixture(scope="function")
def transform_module():
    """
    Reusable fixture for transform tests
    """
    return TransformModule(TRANSFORM_MODULE_PATH)


def test_transform_module(transform_module):
    """
    Test validation of user supplied transform module
    """
    # Valid transform module
    assert transform_module

    # Test that transform_function must be a func
    setattr(transform_module.contents, "transform_function", "hello")

    with pytest.raises(TypeError):
        transform_module._validate()

    # Test that transform_function exist in the module
    delattr(transform_module.contents, "transform_function")

    with pytest.raises(ConfigValidationError):
        transform_module._validate()


def test_no_transform_module(target_api_config):
    """
    Test that when the filepath to the transform function py file is not
    specified, a ConfigValidationError is raised
    """
    with pytest.raises(ConfigValidationError) as e:
        GuidedTransformStage(None)
    assert "Guided transformation requires a" in str(e.value)
