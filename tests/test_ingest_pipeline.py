import os

from click.testing import CliRunner

from kf_lib_data_ingest import cli
from conftest import (
    TEST_DATA_DIR,
    TRANSFORM_MODULE_PATH
)
from conftest import COMMAND_LINE_ERROR_CODE


def test_ingest_cmd_missing_required_args():
    """
    Test ingest CLI when required arguments are missing

    Req args:
        Path to dataset_ingest_config.yml or dir containing the file
    """
    runner = CliRunner()
    result = runner.invoke(cli.ingest, [])

    assert 'dataset_ingest_config_path'.upper() in result.output
    assert result.exit_code == COMMAND_LINE_ERROR_CODE


def test_ingest():
    """
    Test ingest runs successfully with correct params
    """
    # Test auto transform
    ingest_config_path = os.path.join(TEST_DATA_DIR,
                                      'test_study',
                                      'dataset_ingest_config.yml')

    runner = CliRunner()
    result = runner.invoke(cli.ingest, [ingest_config_path,
                                        '--auto_transform'])

    # Test guided transform
    result = runner.invoke(cli.ingest, [ingest_config_path])

    assert result.exit_code == 0
    assert 'BEGIN data ingestion' in result.output
    assert 'END data ingestion' in result.output
