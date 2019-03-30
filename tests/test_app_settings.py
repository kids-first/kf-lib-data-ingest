import os
import re
import shutil
import logging

import pytest
import requests
import requests_mock
from click.testing import CliRunner

from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.app import settings
from conftest import TEST_DATA_DIR

TEST_STUDY_DIR = os.path.join(TEST_DATA_DIR, 'test_study')


@pytest.fixture(scope='function')
def info_caplog(caplog):
    caplog.set_level(logging.INFO)
    return caplog


@pytest.fixture(scope='session')
def extract_config():
    """
    An extract config with a URL that points to the dev study creator API
    in DevelopmentSettings.auth_config
    """
    # Create temp extract config
    src = os.path.join(TEST_STUDY_DIR,
                       'extract_config_w_protected_file.py')
    dest = os.path.join(TEST_STUDY_DIR, 'extract_configs',
                        'extract_config_w_protected_file.py')
    shutil.copyfile(src, dest)

    yield dest

    # Delete file after done
    os.remove(dest)


@pytest.fixture(scope='session')
def cached_schema():
    """
    Cached target api schema
    """
    # Create temp cache_schema file
    src = os.path.join(TEST_DATA_DIR, 'mock_dataservice_schema.json')
    dest = os.path.abspath('cached_schema.json')
    shutil.copyfile(src, dest)

    yield dest

    # Delete after done
    os.remove(dest)


@pytest.fixture(scope='function')
def source_data_file():
    """
    Expected source data file
    """
    fp = os.path.join(TEST_DATA_DIR, 'test_study', 'data',
                      'simple_headered_tsv_1.tsv')
    source_data_file = open(fp, 'rb')
    yield source_data_file
    source_data_file.close()


def _mock_download_file(host, m, filename, expected_content):
    """
    Mock download file from study creator host
    """
    url_regex = re.escape(f"{host}/download/study")
    url_regex = f'^{url_regex}.*$'
    matcher = re.compile(url_regex)
    m.register_uri('GET', matcher, content=expected_content,
                   headers={'Content-Disposition':
                            f'attachment; filename={filename}'})


@pytest.mark.parametrize(
    'app_mode, host',
    [
        ('development', 'http://kf-study-creator-dev.kids-first.io'),
        ('testing', 'http://kf-study-creator-dev.kids-first.io')
    ]
)
def test_dev_modes(info_caplog, app_mode, host, source_data_file,
                   extract_config, cached_schema):
    """
    Test run ingest in `development` and `testing` mode
    """
    os.environ[settings.APP_MODE_ENV_VAR] = app_mode
    settings.setup()

    # Mock request to get protected source file
    with requests_mock.Mocker(real_http=True) as m:
        filename = os.path.basename(source_data_file.name)
        _mock_download_file(host, m, filename, source_data_file.read())

        # Run and test ingest
        runner = CliRunner()
        result = runner.invoke(cli.ingest,
                               [os.path.join(TEST_DATA_DIR, 'test_study')])
        assert result.exit_code == 0
        assert 'BEGIN data ingestion' in result.output
        assert 'END data ingestion' in result.output

        # Ensure the right settings were loaded
        assert 'auth_config' in info_caplog.text
        assert host in info_caplog.text
        assert "'token': 'KF_STUDY_CREATOR_API_TOKEN'" in info_caplog.text

        # Check file was downloaded and used
        extracted_file = os.path.join(
            TEST_STUDY_DIR, 'output', 'ExtractStage',
            os.path.basename(extract_config))
        extracted_file = os.path.splitext(extracted_file)[0] + '.tsv'
        assert os.path.isfile(extracted_file)


def test_production_mode():
    """
    Test run ingest in `production` mode
    """
    # Set environment variables
    os.environ[settings.APP_MODE_ENV_VAR] = 'production'

    with pytest.raises(requests.exceptions.ConnectionError) as e:
        try:
            settings.setup()
            assert 'vault' in str(e)
        except requests.exceptions.ConnectionError as e:
            raise e
