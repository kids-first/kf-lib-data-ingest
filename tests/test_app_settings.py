import os
import re
import shutil
import logging

import pytest
import requests_mock
from click.testing import CliRunner

from kf_lib_data_ingest.app import (
    cli,
    settings
)
from kf_lib_data_ingest.config import ROOT_DIR
from conftest import (
    TEST_DATA_DIR,
    make_ingest_pipeline
)

TEST_STUDY_DIR = os.path.join(TEST_DATA_DIR, 'test_study')
SETTINGS_DIR = os.path.join(ROOT_DIR, 'app', 'settings')


@pytest.fixture(scope='function')
def info_caplog(caplog):
    """
    pytest capture log output at level=INFO
    """
    caplog.set_level(logging.INFO)
    return caplog


@pytest.fixture(scope='function')
def cleanup():
    def delete_leftovers():
        app_modes = ['development', 'production']
        for m in app_modes:
            fp = os.path.join(TEST_STUDY_DIR, 'extract_configs',
                              f'extract_config_{m}.py')
            if os.path.isfile(fp):
                os.remove(fp)
    # Any left over extract configs that should have been deleted
    delete_leftovers()
    yield None
    delete_leftovers()


def extract_config(app_mode):
    """
    Creates an extract config file in test_study/extract_configs

    The extract config to create depends on the app mode
    """
    # Create temp extract config
    src = os.path.join(TEST_STUDY_DIR,
                       f'extract_config_{app_mode}.py')
    dest = os.path.join(TEST_STUDY_DIR, 'extract_configs',
                        f'extract_config_{app_mode}.py')
    return shutil.copyfile(src, dest)


@pytest.fixture(scope='function')
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
    ('development', 'https://kf-study-creator.kidsfirstdrc.org'),
    ('', 'https://kf-study-creator.kidsfirstdrc.org'),
])
def test_non_prod_app_modes(info_caplog, cleanup, source_data_file,
                            cached_schema, app_mode, host):
    """
    Test run ingest for non-production app modes
    """
    # Mock request to get protected source file
    with requests_mock.Mocker(real_http=True) as m:
        filename = os.path.basename(source_data_file.name)
        _mock_download_file(host, m, filename, source_data_file.read())

        # Build CLI params + opts
        params = [os.path.join(TEST_DATA_DIR, 'test_study')]
        # CLI opt
        if app_mode:
            params.extend(['--app_settings',
                           os.path.join(SETTINGS_DIR, app_mode + '.py')])
        # Use default, no cli opt
        else:
            app_mode = 'development'

        # Make temp extract config for tests
        extract_config_filepath = extract_config(app_mode)

        # Run ingest
        runner = CliRunner(env={settings.APP_MODE_ENV_VAR: app_mode})
        result = runner.invoke(cli.ingest, params)

        # Ensure the right settings were loaded
        assert host in info_caplog.text
        assert 'END data ingestion' in info_caplog.text
        assert f'starting in "{app_mode}"' in info_caplog.text

        # Check file was downloaded and used
        extracted_file = os.path.join(
            TEST_STUDY_DIR, 'output', 'ExtractStage',
            os.path.basename(extract_config_filepath))
        extracted_file = os.path.splitext(extracted_file)[0] + '.tsv'
        assert os.path.isfile(extracted_file)


def test_prod_app_mode(info_caplog, cleanup):
    """
    Test run ingest for production (should fail bc we didn't set env vars)
    """
    # Make temp extract config for tests
    extract_config_filepath = extract_config('production')

    # Run and test ingest
    app_mode = 'production'
    app_settings = settings.load(
        os.path.join(SETTINGS_DIR, app_mode + '.py')
    )

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

    # Ingest should fail since nothing set the required env vars
    with pytest.raises(SystemExit):
        try:
            pipeline.run()
        except SystemExit as e:
            raise e
