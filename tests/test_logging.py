import logging
import os

import pytest
from click.testing import CliRunner

from conftest import TEST_LOG_DIR, make_ingest_pipeline
from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.config import (
    DEFAULT_LOG_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT
)
from kf_lib_data_ingest.etl.ingest_pipeline import DataIngestPipeline


def test_defaults(ingest_pipeline):
    """
    Test default logging behavor
    """

    # Test that default log params were set on config
    log_dir = ingest_pipeline.data_ingest_config.log_dir
    assert log_dir == TEST_LOG_DIR
    assert ingest_pipeline.data_ingest_config.log_level == DEFAULT_LOG_LEVEL
    assert (ingest_pipeline.data_ingest_config.overwrite_log ==
            DEFAULT_LOG_OVERWRITE_OPT)

    # Test that ingest.log was created
    ingest_pipeline.run()
    log_filepath = os.path.join(TEST_LOG_DIR, DEFAULT_LOG_FILENAME)
    assert os.path.isfile(log_filepath) is True


def test_log_dir(tmpdir):
    """
    Test non-default log dir
    """
    log_dir = tmpdir.mkdir('my_logs')
    ingest_pipeline = make_ingest_pipeline(log_dir=log_dir.strpath)

    # Run and generate logs
    ingest_pipeline.run()

    # User supplied log dir should exist
    assert len(os.listdir(log_dir)) == 2


def test_overwrite_log(ingest_pipeline):
    """
    Test that overwriting the default log file works
    """
    log_dir = ingest_pipeline.data_ingest_config.log_dir

    # Check for default log files
    ingest_pipeline.run()
    assert len(os.listdir(log_dir)) == 2
    assert os.path.isfile(ingest_pipeline.log_file_path) is True

    # No new files should be created
    ingest_pipeline.run()
    assert len(os.listdir(log_dir)) == 2

    # Test that new logs are created on each pipeline with overwrite_false
    ip = make_ingest_pipeline(overwrite_log=False)
    assert len(os.listdir(log_dir)) == 3
    ip.run()
    assert len(os.listdir(log_dir)) == 4
    ip = make_ingest_pipeline(overwrite_log=False)
    assert len(os.listdir(log_dir)) == 5
    ip.run()
    assert len(os.listdir(log_dir)) == 6


def test_log_level(ingest_pipeline):
    """
    Test that configuration of logging level works
    """
    # Update data ingest config
    ingest_pipeline.data_ingest_config.contents.log_level = 'warning'
    ingest_pipeline.data_ingest_config.contents.overwrite_log = True
    ingest_pipeline.data_ingest_config._set_log_params()

    # Run and generate log
    ingest_pipeline.run()

    # Check log content
    log_filepath = os.path.join(ingest_pipeline.data_ingest_config.log_dir,
                                DEFAULT_LOG_FILENAME)
    with open(log_filepath, 'r') as logfile:
        _check_log_levels(logfile.read(), {'info', 'debug', 'notset'})


def test_cli_log_overrides(caplog):
    """
    Test that CLI option log_level overrides log_level in data_ingest_config
    """
    # Pytest caplog fixture is set to WARNING by default. Set to DEBUG so
    # we can capture log messages
    caplog.set_level(logging._nameToLevel.get('DEBUG'))

    # Run with no override, log level should be info
    runner = CliRunner()
    runner.invoke(cli.ingest, [])
    _check_log_levels(caplog.text, {'debug', 'notset'})

    # Run with cli option override, log level should be warning
    runner.invoke(cli.ingest, ['--log_level', 'warning'])
    _check_log_levels(caplog.text, {'info', 'debug', 'notset'})


def test_log_exceptions(ingest_pipeline):
    """
    Test that exceptions during ingest run are logged and the program exits
    """

    # Create a dummy ingestion pipeline class

    class NewPipeline(DataIngestPipeline):
        # Override run method to throw exception
        def run(self):
            raise Exception('An exception occurred during ingestion!')

    # Create pipeline instance
    p = NewPipeline(
        ingest_pipeline.data_ingest_config.config_filepath,
        ingest_pipeline.target_api_config_path
    )
    p.data_ingest_config.contents.overwrite_log = True
    ingest_pipeline.data_ingest_config._set_log_params()

    # Exception is thrown and log should include exception
    with pytest.raises(Exception):
        p.run()
        log_filepath = os.path.join(p.data_ingest_config.log_dir,
                                    DEFAULT_LOG_FILENAME)

        with open(log_filepath, 'r') as logfile:
            last_line = logfile.readlines()[-1]
            assert 'Exception' in last_line


def _check_log_levels(log_text, levels):
    """
    Check that no log msg in `log_text` has any of the log levels in `levels`
    """
    for line in log_text.split('\n'):
        level = line.split('-')[-1]
        assert level.lower() not in levels
