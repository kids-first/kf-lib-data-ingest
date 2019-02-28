import os
import logging
import pytest
import pandas
import requests_mock

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.misc import read_json
from kf_lib_data_ingest.target_apis import kids_first
from kf_lib_data_ingest.config import KIDSFIRST_DATASERVICE_PROD_URL

from conftest import TEST_DATA_DIR

status_url = f'{KIDSFIRST_DATASERVICE_PROD_URL}/status'
schema_url = f'{KIDSFIRST_DATASERVICE_PROD_URL}/swagger'
mock_dataservice_status = read_json(
    os.path.join(TEST_DATA_DIR, 'mock_dataservice_status.json'))
mock_dataservice_schema = read_json(
    os.path.join(TEST_DATA_DIR, 'mock_dataservice_schema.json'))


@pytest.fixture(scope='function')
def target_instances(transform_stage):
    dfs = {
        'participant': pandas.DataFrame({
            CONCEPT.PARTICIPANT.UNIQUE_KEY: ['p1', 'p2', 'p2'],
            CONCEPT.PARTICIPANT.GENDER: ['Female', 'Male', 'Female']
        }),
        'biospecimen': pandas.DataFrame({
            CONCEPT.BIOSPECIMEN.UNIQUE_KEY: ['b1', 'b2', 'b3'],
            CONCEPT.BIOSPECIMEN.ANALYTE: ['dna', 'rna', 'dna']
        })
    }
    return transform_stage.transformer._standard_to_target(dfs)


@requests_mock.Mocker(kw='mock')
def test_get_kf_schema_server_down(caplog, tmpdir, **kwargs):
    """
    Test kf_lib_data_ingest.target_apis.kids_first._get_kidsfirst_schema

    Test retrieval when server is down and no cached schema exists yet
    """
    url = 'http://localhost:1234'
    mock = kwargs['mock']
    mock.get(f'{url}/status', status_code=500)
    mock.get(f'{url}/swagger', status_code=500)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test retrieval when server is down and no cached schema exists
    cached_schema_file = os.path.join(tmpdir, 'cached_schema.json')
    output = kids_first._get_kidsfirst_schema(
        url, cached_schema_filepath=cached_schema_file)
    assert output is None
    assert 'Unable to retrieve target schema' in caplog.text
    assert not os.path.isfile(cached_schema_file)


@requests_mock.Mocker(kw='mock')
def test_get_kf_schema(caplog, tmpdir, **kwargs):
    """
    Test kf_lib_data_ingest.target_apis.kids_first._get_kidsfirst_schema

    Test retrieval when server is up and no cached schema exists
    Test retrieval when servier is down and cached schema exists
    """
    # Setup mock responses
    mock = kwargs['mock']
    mock.get(status_url, json=mock_dataservice_status)
    mock.get(schema_url, json=mock_dataservice_schema)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test
    cached_schema_file = os.path.join(tmpdir, 'cached_schema.json')
    output = kids_first._get_kidsfirst_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        cached_schema_filepath=cached_schema_file)
    assert output.get('definitions')
    assert output.get('version')
    assert os.path.isfile(cached_schema_file)

    # Test retrieval when server is down and cached schema exists
    mock.get(status_url, status_code=500)
    output = kids_first._get_kidsfirst_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        cached_schema_filepath=cached_schema_file)
    assert output.get('definitions')
    assert output.get('version')


@requests_mock.Mocker(kw='mock')
def test_handle_nulls(caplog, target_instances, **kwargs):
    """
    Test kf_lib_data_ingest.target_apis.kids_first.handle_nulls
    """
    # Setup mock responses
    mock = kwargs['mock']
    mock.get(status_url, json=mock_dataservice_status)
    mock.get(schema_url, json=mock_dataservice_schema)

    target_instances = kids_first.handle_nulls(target_instances)
    expected = {
        # a boolean
        'is_proband': None,
        # a string
        'race': kids_first.VALUE_FOR_NULL_STR,
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
