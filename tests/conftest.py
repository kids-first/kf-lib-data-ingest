import os
import shutil
import pytest


TEST_ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_ROOT_DIR, 'data')
KIDS_FIRST_CONFIG = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                 'src', 'target_apis', 'kids_first.py')


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
    from etl.ingest_pipeline import DataIngestPipeline
    if not config_filepath:
        data_ingest_config_path = os.path.join(TEST_DATA_DIR,
                                               'test_study',
                                               'dataset_ingest_config.yml')

    p = DataIngestPipeline(data_ingest_config_path)

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


@pytest.fixture(scope='function')
def target_api_config():
    from etl.configuration.target_api_config import TargetAPIConfig
    return TargetAPIConfig(KIDS_FIRST_CONFIG)
