import os
import logging
import shutil
import pytest
from unittest import mock

from kf_lib_data_ingest.common.misc import read_json
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.ingest_pipeline import DataIngestPipeline
from kf_lib_data_ingest.etl.transform.auto import AutoTransformStage
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage
from kf_lib_data_ingest.config import DEFAULT_TARGET_URL
TEST_ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_ROOT_DIR, 'data')
TEST_INGEST_OUTPUT_DIR = os.path.join(TEST_DATA_DIR,
                                      'test_study',
                                      'output')
KIDS_FIRST_CONFIG = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                 'kf_lib_data_ingest',
                                 'target_apis', 'kids_first.py')
TRANSFORM_MODULE_PATH = os.path.join(TEST_DATA_DIR,
                                     'test_study',
                                     'transform_module.py')
COMMAND_LINE_ERROR_CODE = 2


def delete_logs(log_dir):
    """
    Delete contents of log dir
    """
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)


def delete_ingest_outputs(output_dir):
    # Delete the entire ingest outputs directory
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)


def make_ingest_pipeline(config_filepath=None):
    """
    Create ingest pipeline as test dependency
    """
    # Create ingest pipeline
    if not config_filepath:
        config_filepath = os.path.join(TEST_DATA_DIR,
                                       'test_study',
                                       'dataset_ingest_config.yml')

    p = DataIngestPipeline(config_filepath)

    # Delete any existing log files
    delete_logs(p.data_ingest_config.log_dir)

    return p


@pytest.fixture(scope='function')
def ingest_pipeline():
    """
    Ingest pipeline fixture
    """
    p = make_ingest_pipeline()

    yield p

    # Teardown
    # Delete the entire log directory
    if os.path.exists(p.data_ingest_config.log_dir):
        shutil.rmtree(p.data_ingest_config.log_dir)

    delete_ingest_outputs(p.ingest_output_dir)


@pytest.fixture(scope='function')
def target_api_config():
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the transform stage and you don't want to worry about setting it up.
    """
    return TargetAPIConfig(KIDS_FIRST_CONFIG)


@pytest.fixture(scope='function')
def guided_transform_stage(caplog):
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the transform stage and you don't want to worry about setting it up.
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.DEBUG)

    # Before test setup
    # Mock get_open_api_v2_schema to always return the schema
    mock_dataservice_schema = read_json(
        os.path.join(TEST_DATA_DIR, 'mock_dataservice_schema.json'))
    patcher = mock.patch(
        'kf_lib_data_ingest.common.misc.get_open_api_v2_schema',
        return_value=mock_dataservice_schema)
    patcher.start()
    yield GuidedTransformStage(
        TRANSFORM_MODULE_PATH, KIDS_FIRST_CONFIG,
        target_api_url=DEFAULT_TARGET_URL,
        ingest_output_dir=TEST_INGEST_OUTPUT_DIR)

    # After test teardown
    patcher.stop()
    delete_ingest_outputs(TEST_INGEST_OUTPUT_DIR)

@pytest.fixture(scope='function')
def auto_transform_stage(caplog):
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the transform stage and you don't want to worry about setting it up.
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.DEBUG)

    # Before test setup
    # Mock get_open_api_v2_schema to always return the schema
    mock_dataservice_schema = read_json(
        os.path.join(TEST_DATA_DIR, 'mock_dataservice_schema.json'))
    patcher = mock.patch(
        'kf_lib_data_ingest.common.misc.get_open_api_v2_schema',
        return_value=mock_dataservice_schema)
    patcher.start()
    yield AutoTransformStage(
        KIDS_FIRST_CONFIG,
        target_api_url=DEFAULT_TARGET_URL,
        ingest_output_dir=TEST_INGEST_OUTPUT_DIR)
    # After test teardown
    patcher.stop()
    delete_ingest_outputs(TEST_INGEST_OUTPUT_DIR)