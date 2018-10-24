import os
import time

import pytest

from config import (
    DEFAULT_LOG_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT,
    INGEST_OP,
    DEFAULT_TARGET_URL
)
from conftest import (
    TEST_ROOT_DIR,
    TEST_DATA_DIR,
    KIDS_FIRST_CONFIG
)

target_api_config_path = KIDS_FIRST_CONFIG
default_log_dir = os.path.join(TEST_DATA_DIR, 'test_study', 'logs')


def test_defaults(ingest_pipeline):
    """
    Test default logging behavor
    """

    # Test that default log params were set on config
    log_dir = ingest_pipeline.dataset_ingest_config.log_dir
    assert log_dir == default_log_dir
    assert ingest_pipeline.dataset_ingest_config.log_level == DEFAULT_LOG_LEVEL
    assert (ingest_pipeline.dataset_ingest_config.overwrite_log ==
            DEFAULT_LOG_OVERWRITE_OPT)

    # Test that ingest.log was not created but ingest-{datetime}.log
    # was created
    _invoke_pipeline(ingest_pipeline)
    log_filepath = os.path.join(default_log_dir, DEFAULT_LOG_FILENAME)
    assert os.path.isfile(log_filepath) == False

    # Test that new logs are created on each run
    time.sleep(1)
    _invoke_pipeline(ingest_pipeline)
    assert len(os.listdir(default_log_dir)) == 2


def test_log_dir(ingest_pipeline):
    """
    Test non-default log dir
    """
    # Create log dir
    log_dir = os.path.join(TEST_ROOT_DIR, 'mylogs')
    os.mkdir(log_dir)
    # Update data ingest config
    ingest_pipeline.dataset_ingest_config.contents['logging'] = {
        'log_dir': log_dir}
    ingest_pipeline.dataset_ingest_config._set_log_params()
    # Run and generate logs
    _invoke_pipeline(ingest_pipeline)

    # User supplied log dir should exist
    assert len(os.listdir(log_dir)) == 1


def test_overwrite_log(ingest_pipeline):
    """
    Test that overwriting the default log file works
    """

    # Update data ingest config
    ingest_pipeline.dataset_ingest_config.contents['logging'] = {
        'overwrite_log': True}
    ingest_pipeline.dataset_ingest_config._set_log_params()

    log_dir = ingest_pipeline.dataset_ingest_config.log_dir
    assert len(os.listdir(log_dir)) == 0

    # Run and generate logs
    _invoke_pipeline(ingest_pipeline)

    # Check for default log file
    assert len(os.listdir(log_dir)) == 1
    log_filepath = os.path.join(log_dir, DEFAULT_LOG_FILENAME)
    assert os.path.isfile(log_filepath) == True

    # No new files should be created
    _invoke_pipeline(ingest_pipeline)
    assert len(os.listdir(log_dir)) == 1


def test_log_level(ingest_pipeline):
    """
    Test that configuration of logging level works
    """
    # Update data ingest config
    ingest_pipeline.dataset_ingest_config.contents['logging'] = {
        'log_level': 'warning',
        'overwrite_log': True
    }
    ingest_pipeline.dataset_ingest_config._set_log_params()

    # Run and generate log
    _invoke_pipeline(ingest_pipeline)

    # Check log content
    log_filepath = os.path.join(ingest_pipeline.dataset_ingest_config.log_dir,
                                DEFAULT_LOG_FILENAME)
    with open(log_filepath, 'r') as logfile:
        for line in logfile.readlines():
            level = line.split('-')[-1].lower()
            assert level not in {'info', 'debug', 'notset'}


def test_log_exceptions(ingest_pipeline):
    """
    Test that exceptions during ingest run are logged and the program exits
    """

    # Create a dummy ingestion pipeline class
    from etl.ingest_pipeline import DataIngestPipeline

    class NewPipeline(DataIngestPipeline):
        # Override run method to throw exception
        def invoke(self):
            raise Exception('An exception occurred during ingestion!')

    # Create pipeline instance
    p = NewPipeline(ingest_pipeline.dataset_ingest_config.config_filepath)
    p.dataset_ingest_config.contents['logging'] = {
        'overwrite_log': True
    }
    ingest_pipeline.dataset_ingest_config._set_log_params()

    # Exception is thrown and log should include exception
    with pytest.raises(Exception):
        p.invoke(INGEST_OP, target_api_config_path)
        log_filepath = os.path.join(p.dataset_ingest_config.log_dir,
                                    DEFAULT_LOG_FILENAME)

        with open(log_filepath, 'r') as logfile:
            last_line = logfile.readlines()[-1]
            assert 'Exception' in last_line


def _invoke_pipeline(ingest_pipeline):
    ingest_pipeline.invoke(INGEST_OP, target_api_config_path,
                           DEFAULT_TARGET_URL, False, None, True)
