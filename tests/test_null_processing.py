import os
import logging
import pytest
import pandas
import requests_mock

from kf_lib_data_ingest.etl.transform.transform import TransformStage
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.misc import (
    read_json,
    get_open_api_v2_schema
)
from kf_lib_data_ingest.config import KIDSFIRST_DATASERVICE_PROD_URL

from conftest import (
    TEST_DATA_DIR,
    TEST_INGEST_OUTPUT_DIR,
    KIDS_FIRST_CONFIG,
    TRANSFORM_MODULE_PATH
)

schema_url = f'{KIDSFIRST_DATASERVICE_PROD_URL}/swagger'
mock_dataservice_schema = read_json(
    os.path.join(TEST_DATA_DIR, 'mock_dataservice_schema.json'))

logger = logging.getLogger('test_logger')


@pytest.fixture(scope='function')
def target_instances(guided_transform_stage):
    dfs = {
        'participant': pandas.DataFrame({
            CONCEPT.PARTICIPANT.UNIQUE_KEY: ['p1', 'p2', 'p2'],
            CONCEPT.PARTICIPANT.GENDER: ['Female', 'Male', 'Female']
        }),
        'biospecimen': pandas.DataFrame({
            CONCEPT.PARTICIPANT.UNIQUE_KEY: ['p1', 'p2', 'p2'],
            CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ['b1', 'b2', 'b3'],
            CONCEPT.BIOSPECIMEN.ANALYTE: ['dna', 'rna', 'dna']
        })
    }
    all_data_df = pandas.merge(dfs['participant'], dfs['biospecimen'],
                               on=CONCEPT.PARTICIPANT.UNIQUE_KEY)
    return guided_transform_stage._standard_to_target(all_data_df)


@pytest.fixture(scope='function')
@requests_mock.Mocker(kw='mock')
def schema(tmpdir, target_api_config, **kwargs):
    # Setup mock responses
    mock = kwargs['mock']
    mock.get(schema_url, json=mock_dataservice_schema)

    cached_schema_file = os.path.join(tmpdir, 'cached_schema.json')
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        target_api_config.concept_schemas.keys(),
        cached_schema_filepath=cached_schema_file)

    return output


@requests_mock.Mocker(kw='mock')
def test_get_kf_schema_server_down(caplog, tmpdir, **kwargs):
    """
    Test kf_lib_data_ingest.target_apis.get_open_api_v2_schema

    Test retrieval when server is down and no cached schema exists yet
    """
    url = 'http://localhost:1234'
    mock = kwargs['mock']
    mock.get(f'{url}/swagger', status_code=500)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test retrieval when server is down and no cached schema exists
    cached_schema_file = os.path.join(tmpdir, 'cached_schema.json')
    output = get_open_api_v2_schema(
        url, [], cached_schema_filepath=cached_schema_file, logger=logger)
    assert output is None
    assert 'Unable to retrieve target schema' in caplog.text
    assert not os.path.isfile(cached_schema_file)


@requests_mock.Mocker(kw='mock')
def test_get_kf_schema(caplog, tmpdir, target_api_config, **kwargs):
    """
    Test kf_lib_data_ingest.common.misc.get_open_api_v2_schema

    Test retrieval when server is up and no cached schema exists
    Test retrieval when servier is down and cached schema exists
    """
    # Setup mock responses
    mock = kwargs['mock']
    mock.get(schema_url, json=mock_dataservice_schema)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test server up, no cache file exists
    cached_schema_file = os.path.join(tmpdir, 'cached_schema.json')
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        target_api_config.concept_schemas.keys(),
        cached_schema_filepath=cached_schema_file)
    assert output.get('definitions')
    assert output.get('version')
    assert output.get('target_service')
    assert os.path.isfile(cached_schema_file)

    # Test retrieval when server is down and cached schema exists
    mock.get(schema_url, status_code=500)
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        target_api_config.concept_schemas.keys(),
        cached_schema_filepath=cached_schema_file)
    assert output.get('definitions')
    assert output.get('version')
    assert output.get('target_service')

    # Test cached schema file created in default loc
    mock.get(schema_url, json=mock_dataservice_schema)
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        target_api_config.concept_schemas.keys())
    assert os.path.isfile(os.path.realpath('./cached_schema.json'))


def test_handle_nulls(
    caplog, guided_transform_stage, target_instances, schema
):
    """
    Test kf_lib_data_ingest.etl.transform.transform.handle_nulls

    Normal operation
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test normal operation
    guided_transform_stage.handle_nulls(target_instances, schema)
    expected = {
        # a boolean
        'is_proband': None,
        # a string
        'race': constants.COMMON.NOT_REPORTED,
        # an int/float
        'age_at_event_days': None,
        # a datetime
        'shipement_date': None
    }

    for target_concept, instances in target_instances.items():
        for instance in instances:
            for attr, value in instance.get('properties', {}).items():
                if attr in expected:
                    assert value == expected[attr]


def test_handle_nulls_no_schema(
    caplog, guided_transform_stage, target_instances, schema
):
    """
    Test kf_lib_data_ingest.etl.transform.transform.handle_nulls

    When no schema exists for a target_concept, 'participant'
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Handle nulls
    schema['definitions'].pop('participant')
    guided_transform_stage.handle_nulls(target_instances, schema)
    assert ('Skip handle nulls for participant. No schema was found.' in
            caplog.text)


def test_handle_nulls_no_prop_def(
    caplog, guided_transform_stage, target_instances, schema
):
    """
    Test kf_lib_data_ingest.etl.transform.transform.handle_nulls

    When no property def exists in schema for a property, participant.gender
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test setup
    schema['definitions']['participant']['properties'].pop('gender')
    target_instances['participant'][0]['properties']['gender'] = None

    # Handle nulls
    guided_transform_stage.handle_nulls(target_instances, schema)
    assert ('No property definition found for '
            f'participant.gender in target schema ' in
            caplog.text)
