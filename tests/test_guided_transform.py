import logging
import pytest
import pandas as pd

from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.configuration.transform_module import (
    TransformModule
)
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformer
from kf_lib_data_ingest.common.concept_schema import CONCEPT

from conftest import TRANSFORM_MODULE_PATH


@pytest.fixture(scope='function')
def dfs():
    """
    Mock input to GuidedTransformer.run
    """
    return {
        'family': pd.DataFrame({
            CONCEPT.FAMILY.UNIQUE_KEY: ['f1', 'f2', 'f3']
        }),
        'participant': pd.DataFrame({
            CONCEPT.PARTICIPANT.UNIQUE_KEY: ['p1', 'p2', 'p2'],
            CONCEPT.PARTICIPANT.GENDER: ['Female', 'Male', 'Female']
        }),
        'biospecimen': pd.DataFrame({
            CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ['b1', 'b2', 'b3'],
            CONCEPT.BIOSPECIMEN.ANALYTE: ['dna', 'rna', 'dna']
        }),
        'sequencing_experiment': pd.DataFrame({
            CONCEPT.SEQUENCING.LIBRARY_NAME: ['lib1', 'lib2', 'lib3']
        }),
    }


@pytest.fixture(scope='function')
def transform_module():
    """
    Reusable fixture for transform tests
    """
    return TransformModule(TRANSFORM_MODULE_PATH)


def test_standard_to_target_transform(caplog, dfs, transform_stage):
    """
    Test standard to target concept transformation
    """
    caplog.set_level(logging.INFO)
    # Transform
    guided_transformer = transform_stage.transformer
    target_instances = guided_transformer._standard_to_target(dfs)

    # Check that output only contains concepts that had data and unique key
    output_concepts = target_instances.keys()
    expected_concepts = [c for c in output_concepts
                         if c != 'sequencing_experiment']
    assert not (set(output_concepts)
                .symmetric_difference(set(expected_concepts)))

    # Check instances counts and values
    for target_concept, instances in target_instances.items():
        # Only 2 unique participants
        if target_concept == 'participant':
            assert len(instances) == 2
        else:
            input_rows = dfs.get(target_concept).shape[0]
            assert input_rows == len(instances)

        for instance in instances:
            assert instance.get('id')
            assert instance.get('properties')
            assert 'links' in instance

    # Check log output
    no_data_concepts = (
        set(transform_stage.target_api_config.concept_schemas.keys())
        .symmetric_difference(set(expected_concepts))
    )
    no_table_msg = 'No table found for target concept:'
    no_unique_key_msg = 'No unique key found in table for target concept:'
    for c in no_data_concepts:
        if c == 'sequencing_experiment':
            expected_log = f'{no_unique_key_msg} {c}'
        else:
            expected_log = f'{no_table_msg} {c}'

        assert expected_log in caplog.text


def test_transform_module(transform_module):
    """
    Test validation of user supplied transform module
    """
    # Valid transform module
    assert transform_module

    # Test that transform_function must be a func
    setattr(transform_module.contents, 'transform_function', 'hello')

    with pytest.raises(TypeError):
        transform_module._validate()

    # Test that transform_function exist in the module
    delattr(transform_module.contents, 'transform_function')

    with pytest.raises(ConfigValidationError):
        transform_module._validate()


def test_no_transform_module(target_api_config):
    """
    Test that when the filepath to the transform function py file is not
    specified, a ConfigValidationError is raised
    """
    with pytest.raises(ConfigValidationError) as e:
        GuidedTransformer(target_api_config, None)
        assert 'Guided transformation requires a' in str(e)


@pytest.mark.parametrize('ret_val, error',
                         [(None, TypeError),
                          ({'foo': pd.DataFrame()}, KeyError),
                          ({'participant': None}, TypeError)
                          ])
def test_bad_ret_vals_transform_funct(transform_stage, ret_val, error):
    """
    Test wrong return values from transform function
    """
    tf = transform_stage.transformer

    def f(df_dict):
        return ret_val

    tf.transform_module.transform_function = f
    with pytest.raises(error):
        tf._apply_transform_funct({})
