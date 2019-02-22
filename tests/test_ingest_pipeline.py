import os
import shutil

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


def test_ingest(tmpdir):
    """
    Test ingest - guided transform
    """
    ingest_config_path = os.path.join(TEST_DATA_DIR, 'test_study',
                                      'dataset_ingest_config.yml')

    runner = CliRunner()
    result = runner.invoke(cli.ingest, [ingest_config_path])

    assert result.exit_code == 0
    assert 'BEGIN data ingestion' in result.output
    assert 'END data ingestion' in result.output


def test_ingest_no_transform_module(tmpdir):
    """
    Test ingest with no transform module defined
    """
    # Copy test_study dir and remove the transform module
    study_dir = os.path.join(TEST_DATA_DIR, 'test_study')
    new_study_dir = os.path.join(tmpdir, 'study')
    shutil.copytree(study_dir, new_study_dir)
    tm = os.path.join(new_study_dir, 'transform_module.py')
    if os.path.isfile(tm):
        os.remove(tm)

    # Remove transform module from ingest config yaml
    ingest_config_path = os.path.join(new_study_dir,
                                      'dataset_ingest_config.yml')
    with open(ingest_config_path) as txtfile:
        lines = [line
                 for line in txtfile.readlines()
                 if not line.strip().startswith('transform_function_path')]
    with open(ingest_config_path, 'w') as txtfile:
        txtfile.write(''.join(lines))

    # Run ingest
    runner = CliRunner()
    result = runner.invoke(cli.ingest, [ingest_config_path])
    assert result.exit_code > 0
    assert 'Transform module file has not been created yet' in result.output
