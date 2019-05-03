import os
import shutil

import pytest
from click.testing import CliRunner

from kf_lib_data_ingest.config import VERSION
from kf_lib_data_ingest.app import cli
from conftest import TEST_DATA_DIR
from conftest import COMMAND_LINE_ERROR_CODE

TEST_STUDY_CONFIG = os.path.join(TEST_DATA_DIR, 'test_study',
                                 'ingest_package_config.py')


def test_ingest_cmd_missing_required_args():
    """
    Test ingest CLI when required arguments are missing

    Req args:
        Path to ingest_package_config.py or dir containing the file
    """
    runner = CliRunner()
    result = runner.invoke(cli.ingest, [])

    assert 'ingest_package_config_path'.upper() in result.output
    assert result.exit_code == COMMAND_LINE_ERROR_CODE


@pytest.mark.parametrize(
    'cli_cmd, arg_list',
    [
        (cli.test, [TEST_STUDY_CONFIG, '--log_level', 'debug']),
        (cli.ingest, [TEST_STUDY_CONFIG, '--dry_run', '--log_level', 'debug'])
    ]
)
def test_ingest_cmds(cli_cmd, arg_list):
    """
    Test ingest and test CLI commands - guided transform
    """
    runner = CliRunner()
    result = runner.invoke(cli_cmd, arg_list)
    assert result.exit_code == 1

    assert 'BEGIN data ingestion' in result.output
    assert 'END data ingestion' in result.output
    assert 'version' in result.output
    assert 'commit' in result.output
    assert f'{VERSION}' in result.output

    # Make sure that post-extract counts run
    assert 'Begin Basic Stage Output Validation' in result.output
    assert (
        "UNIQUE COUNTS:\n{'BIOSPECIMEN|ALIQUOT_ID': 64,"
        in result.output
    )
    assert ('| BIOSPECIMEN|ID |         60 |      60 | ✅' in
            result.output)

    assert 'DRY' in result.output


def test_ingest_no_transform_module(tmpdir):
    """
    Test ingest with no transform module defined

    Note - This test is temporary. When auto-transform implementation is
    complete, the absence of a transform module will result in the execution
    of auto-transform. This test will be modified to check that auto-transform
    executes.
    """
    # Copy test_study dir and remove the transform module
    study_dir = os.path.join(TEST_DATA_DIR, 'test_study')
    new_study_dir = os.path.join(tmpdir, 'study')
    shutil.copytree(study_dir, new_study_dir)
    tm = os.path.join(new_study_dir, 'transform_module.py')
    if os.path.isfile(tm):
        os.remove(tm)

    # Remove transform module from ingest config
    ingest_config_path = os.path.join(new_study_dir,
                                      'ingest_package_config.py')
    with open(ingest_config_path) as txtfile:
        lines = [line
                 for line in txtfile.readlines()
                 if not line.strip().startswith('transform_function_path')]
    with open(ingest_config_path, 'w') as txtfile:
        txtfile.write(''.join(lines))

    # Run ingest
    runner = CliRunner()
    result = runner.invoke(cli.ingest, f'{ingest_config_path} --dry_run')
    assert result.exit_code > 0
    assert 'Transform module file has not been created yet' in result.output
