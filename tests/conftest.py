import os
import logging
import shutil
import pytest
from unittest import mock

from kf_lib_data_ingest.common.misc import read_json
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)
from kf_lib_data_ingest.etl.ingest_pipeline import DataIngestPipeline
from kf_lib_data_ingest.etl.transform.auto import AutoTransformStage
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage
from kf_lib_data_ingest.config import DEFAULT_TARGET_URL

TEST_ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_DATA_DIR = os.path.join(TEST_ROOT_DIR, "data")
TEST_INGEST_OUTPUT_DIR = os.path.join(TEST_DATA_DIR, "simple_study", "output")
KIDS_FIRST_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "kf_lib_data_ingest",
    "target_apis",
    "kids_first.py",
)
TRANSFORM_MODULE_PATH = os.path.join(
    TEST_DATA_DIR, "simple_study", "transform_module.py"
)
TEST_LOG_DIR = os.path.join(TEST_DATA_DIR, "simple_study", "logs")
TEST_INGEST_CONFIG = os.path.join(
    TEST_DATA_DIR, "simple_study", "ingest_package_config.py"
)

COMMAND_LINE_ERROR_CODE = 2

KIDSFIRST_DATASERVICE_PROD_URL = "http://kf-api-dataservice.kidsfirstdrc.org"
TEST_AUTH0_DOMAIN = "natashasingh.auth0.com"
TEST_AUTH0_AUD = "https://test-api.kids-first.io/files"
TEST_CLIENT_ID = "jvpfU40lDRRaRSMEZ0C9FKm379H176W6"
TEST_CLIENT_SECRET = (
    "B0v9vADfY9A7KQFxYxkoMHySuL6l6v7_eQb7s-3tv3MSSY4Pwtkmrv_vJ6VDbbpB"
)  # noqa E501

# Mock get_open_api_v2_schema to always return the schema
mock_dataservice_schema = read_json(
    os.path.join(TEST_DATA_DIR, "mock_dataservice_schema.json")
)


@pytest.fixture(scope="function")
def info_caplog(caplog):
    """
    pytest capture log output at level=INFO
    """
    caplog.set_level(logging.INFO)
    return caplog


def delete_dir_contents(dir):
    """
    Delete contents of a dir
    """
    if os.path.isdir(dir):
        for filename in os.listdir(dir):
            file_path = os.path.join(dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)


def delete_dir(dir):
    """
    Delete an entire dir
    """
    if os.path.exists(dir):
        shutil.rmtree(dir)


def make_ingest_pipeline(
    config_filepath=None,
    target_api_config_path=None,
    log_dir=None,
    overwrite_log=None,
    auth_configs=None,
):
    """
    Create ingest pipeline as test dependency
    """
    # Create ingest pipeline
    if not config_filepath:
        config_filepath = TEST_INGEST_CONFIG

    if not log_dir:
        log_dir = TEST_LOG_DIR

    if not target_api_config_path:
        target_api_config_path = KIDS_FIRST_CONFIG

    return DataIngestPipeline(
        config_filepath,
        target_api_config_path=target_api_config_path,
        log_dir=log_dir,
        overwrite_log=overwrite_log,
        dry_run=True,
        auth_configs=auth_configs,
    )


@pytest.fixture(scope="function")
def ingest_pipeline():
    """
    Ingest pipeline fixture
    """
    # Delete any existing log files
    delete_dir_contents(TEST_LOG_DIR)

    p = make_ingest_pipeline()

    yield p

    # Teardown
    delete_dir(p.data_ingest_config.log_dir)
    delete_dir(p.ingest_output_dir)


@pytest.fixture(scope="function")
def target_api_config():
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the transform stage and you don't want to worry about setting it up.
    """
    return TargetAPIConfig(KIDS_FIRST_CONFIG)


@pytest.fixture(scope="function")
def guided_transform_stage(caplog):
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the guided transform stage and you don't want to worry about setting it up.
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.DEBUG)

    patcher = mock.patch(
        "kf_lib_data_ingest.network.utils.get_open_api_v2_schema",
        return_value=mock_dataservice_schema,
    )

    patcher.start()
    yield GuidedTransformStage(
        os.path.join(
            TEST_DATA_DIR, "simple_study", "transform_module_simple.py"
        ),
        KIDS_FIRST_CONFIG,
        target_api_url=DEFAULT_TARGET_URL,
        ingest_output_dir=TEST_INGEST_OUTPUT_DIR,
    )
    patcher.stop()

    delete_dir(TEST_INGEST_OUTPUT_DIR)


@pytest.fixture(scope="function")
def auto_transform_stage(caplog):
    """
    Re-usable fixture for tests. Use this one for all tests that need
    the auto transform stage and you don't want to worry about setting it up.
    """
    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.DEBUG)

    patcher = mock.patch(
        "kf_lib_data_ingest.network.utils.get_open_api_v2_schema",
        return_value=mock_dataservice_schema,
    )

    patcher.start()
    yield AutoTransformStage(
        KIDS_FIRST_CONFIG,
        target_api_url=DEFAULT_TARGET_URL,
        ingest_output_dir=TEST_INGEST_OUTPUT_DIR,
    )
    patcher.stop()

    delete_dir(TEST_INGEST_OUTPUT_DIR)
