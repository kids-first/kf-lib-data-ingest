import os
import shutil
import pytest


from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)
from kf_lib_data_ingest.etl.ingest_pipeline import DataIngestPipeline
from kf_lib_data_ingest.etl.transform.transform import TransformStage
TEST_ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_ROOT_DIR, 'data')
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

    # Delete the entire ingest outputs directory
    if os.path.exists(p.ingest_output_dir):
        shutil.rmtree(p.ingest_output_dir)


@pytest.fixture(scope='function')
def target_api_config():
    return TargetAPIConfig(KIDS_FIRST_CONFIG)


@pytest.fixture(scope='function')
def transform_stage():
    return TransformStage(KIDS_FIRST_CONFIG,
                          transform_function_path=TRANSFORM_MODULE_PATH)
