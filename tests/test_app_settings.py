import os
import re
import shutil
import logging

import pytest
import requests_mock

from kf_lib_data_ingest.app import settings
from conftest import (
    TEST_DATA_DIR,
    make_ingest_pipeline
)

TEST_STUDY_DIR = os.path.join(TEST_DATA_DIR, 'test_study')


@pytest.fixture(scope='function')
def info_caplog(caplog):
    """
    pytest capture log output at level=INFO
    """
    caplog.set_level(logging.INFO)
    return caplog


@pytest.fixture(scope='function')
def extract_config():
    """
    Return a func pointer to a func that creates an extract config file
    in test_study/extract_configs

    The extract config to create depends on the app mode

    Delete copy when test finishes
    """
    # Create temp extract config
    dest = os.path.join(TEST_STUDY_DIR, 'extract_configs',
                        f'extract_config_protected_file.py')

    def make_extract_config(app_mode):
        src = os.path.join(TEST_STUDY_DIR,
                           f'extract_config_{app_mode}.py')
        shutil.copyfile(src, dest)

        return dest

    yield make_extract_config

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


@pytest.mark.parametrize('app_mode, host', [
    ('development', 'http://kf-study-creator-dev.kids-first.io'),
    ('testing', 'https://kf-study-creator.kidsfirstdrc.org')
])
def test_non_prod_app_modes(info_caplog, source_data_file, extract_config,
                            cached_schema, app_mode, host):
    """
    Test run ingest for non-production app modes
    """
    # Make temp extract config for tests
    extract_config_filepath = extract_config(app_mode)

    # Mock request to get protected source file
    with requests_mock.Mocker(real_http=True) as m:
        filename = os.path.basename(source_data_file.name)
        _mock_download_file(host, m, filename, source_data_file.read())

        # Run ingest
        app_settings = settings.load(app_mode)
        pipeline = make_ingest_pipeline(
            config_filepath=os.path.join(TEST_DATA_DIR, 'test_study'),
            target_api_config_path=app_settings.TARGET_API_CONFIG,
            auth_configs=app_settings.AUTH_CONFIGS)
        pipeline.run()

        # Ensure the right settings were loaded
        assert app_settings.APP_MODE == app_mode
        assert 'auth_config' in info_caplog.text
        assert host in info_caplog.text
        assert 'END data ingestion' in info_caplog.text

        # Check file was downloaded and used
        extracted_file = os.path.join(
            TEST_STUDY_DIR, 'output', 'ExtractStage',
            os.path.basename(extract_config_filepath))
        extracted_file = os.path.splitext(extracted_file)[0] + '.tsv'
        assert os.path.isfile(extracted_file)


def test_prod_app_mode(info_caplog, extract_config):
    """
    Test run ingest for production (should fail bc we didn't set env vars)
    """
    # Make temp extract config for tests
    extract_config('production')

    # Run and test ingest
    app_mode = 'production'
    app_settings = settings.load(app_mode)

    # Pretend that env vars were not set
    for url, cfg in app_settings.AUTH_CONFIGS.items():
        for key, val in cfg.items():
            if key != 'type':
                cfg[key] = None
    # Run ingest
    pipeline = make_ingest_pipeline(
        config_filepath=os.path.join(TEST_DATA_DIR, 'test_study'),
        target_api_config_path=app_settings.TARGET_API_CONFIG,
        auth_configs=app_settings.AUTH_CONFIGS)

    # Ensure the right settings were loaded
    assert app_settings.APP_MODE == app_mode

    # Ingest should fail since nothing set the required env vars
    with pytest.raises(SystemExit):
        try:
            pipeline.run()
        except SystemExit as e:
            raise e
