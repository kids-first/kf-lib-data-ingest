import os
import logging
from urllib.parse import (
    urljoin,
    urlencode
)

import boto3
import pytest
import requests_mock
from moto import mock_s3
from requests.auth import HTTPBasicAuth

from kf_lib_data_ingest.common.file_retriever import FileRetriever
from conftest import (
    TEST_AUTH0_DOMAIN,
    TEST_AUTH0_AUD,
    TEST_CLIENT_ID,
    TEST_CLIENT_SECRET,
    TEST_DATA_DIR
)
from mocks import OAuth2Mocker

TEST_S3_BUCKET = "s3_bucket"
TEST_S3_PREFIX = "mock_folder"
TEST_FILENAME = 'data.csv'
TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, TEST_FILENAME)
TEST_PARAMS = [
    (False, True, False),
    (False, False, False),
    (True, False, True),
    (True, True, False)
]


@pytest.fixture(scope='session')
def storage_dir(tmpdir_factory):
    """
    Uses pytest tmpdir_factory fixture for temporarily storing
    file getter files. Used for duration of these tests and then
    deleted after tests complete
    """
    return tmpdir_factory.mktemp('retrieved_files')


@pytest.fixture(scope='function')
def s3_file():
    """
    Mock bucket creation, file upload. Return path to file on s3.
    """
    mock_s3().start()
    # Create bucket
    client = boto3.client("s3",
                          region_name="eu-west-1",
                          aws_access_key_id="fake_access_key",
                          aws_secret_access_key="fake_secret_key")
    client.create_bucket(Bucket=TEST_S3_BUCKET)

    # Upload file
    client.upload_file(Filename=TEST_FILE_PATH,
                       Bucket=TEST_S3_BUCKET,
                       Key=f'{TEST_S3_PREFIX}/{TEST_FILENAME}')
    # Sanity check - bucket exists
    response = client.head_bucket(Bucket=TEST_S3_BUCKET)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    yield f's3://{TEST_S3_BUCKET}/{TEST_S3_PREFIX}/{TEST_FILENAME}'

    # Teardown
    mock_s3().stop()


@pytest.fixture(scope='function')
def auth_config():
    auth_config = {
        'https://api.com/foo': {
            'type': 'basic',
            'variable_names': {
                'username': 'FOO_SERVICE_UNAME',
                'password': 'FOO_SERVICE_PW'
            }
        },
        'https://test-api.kids-first.io/files/download': {
            'type': 'oauth2',
            'variable_names': {
                'provider_domain': 'TEST_AUTH0_DOMAIN',
                'audience': 'TEST_AUTH0_AUD',
                'client_id': 'TEST_CLIENT_ID',
                'client_secret': 'TEST_CLIENT_SECRET'
            }
        },
        'https://kf-study-creator.kidsfirstdrc.org/download/study': {
            'type': 'token',
            'token_location': 'url',
            'variable_names': {
                'token': 'TEST_API_TOKEN'
            }
        },
        'https://api.com/bar': {
            'type': 'token',
            'variable_names': {
                'token': 'TEST_API_TOKEN'
            }
        }

    }
    os.environ['FOO_SERVICE_UNAME'] = 'username'
    os.environ['FOO_SERVICE_PW'] = 'password'
    os.environ['TEST_AUTH0_DOMAIN'] = TEST_AUTH0_DOMAIN
    os.environ['TEST_AUTH0_AUD'] = TEST_AUTH0_AUD
    os.environ['TEST_CLIENT_ID'] = TEST_CLIENT_ID
    os.environ['TEST_CLIENT_SECRET'] = TEST_CLIENT_SECRET
    os.environ['TEST_API_TOKEN'] = 'a developer token'

    return auth_config


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist',
                         TEST_PARAMS
                         )
def test_s3_file(s3_file, storage_dir, use_storage_dir, cleanup_at_exit,
                 should_file_exist):
    """
    Test file retrieval via s3
    """
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "foobar_key")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "foobar_secret")
    _test_get_file(s3_file, storage_dir, use_storage_dir, cleanup_at_exit,
                   should_file_exist)


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist',
                         TEST_PARAMS
                         )
def test_get_web(caplog, storage_dir, use_storage_dir, cleanup_at_exit,
                 should_file_exist):
    """
    Test file retrieval via http
    """
    # With Content-Disposition header containing file ext
    url = f'http://{os.path.basename(TEST_FILENAME)}/download'
    file_ext = os.path.splitext(TEST_FILENAME)[-1]
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get(url, content=tf.read(),
                  headers={'Content-Disposition':
                           f'attachment; filename={TEST_FILENAME}'})
            _test_get_file(url, storage_dir,
                           use_storage_dir, cleanup_at_exit,
                           should_file_exist, expected_file_ext=file_ext)
    # Without Content-Disposition header
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get(url, content=tf.read())
            _test_get_file(url, storage_dir,
                           use_storage_dir, cleanup_at_exit,
                           should_file_exist, expected_file_ext='')
    assert f'{url}' in caplog.text
    assert 'Content-Disposition' in caplog.text


@pytest.mark.parametrize(
    'url, auth, use_auth_config, expected_log', [
        ('http://www.example.com', None, False, ''),
        ('https://api.com/foo/1', None, True,
         'Selected `basic` authentication'),
        ('https://api.com/foo/1', HTTPBasicAuth('username', 'password'),
         False, 'Using `basic` authentication'),
        ('https://another-api.com/files', None, True,
         'Authentication scheme not found'),
        ('https://test-api.kids-first.io/files/download/file_id',
         None, True, 'Selected `oauth2` authentication'),
        ('https://kf-study-creator.kidsfirstdrc.org/download/study',
         None, True, 'Selected `token` authentication'),
        ('https://api.com/bar', None, True, 'Selected `token` authentication')
    ])
def test_get_web_w_auth(caplog, tmpdir, auth_config,
                        url, auth, use_auth_config, expected_log):

    caplog.set_level(logging.INFO)
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            file_content = tf.read()
            m.get(url, content=file_content)

            get_kwargs = {}
            if use_auth_config:
                get_kwargs['auth_config'] = auth_config

            # Basic auth or no auth
            if '`basic`' in expected_log:
                if auth:
                    get_kwargs = {
                        'auth': auth
                    }
                _test_get_file(url, tmpdir, True, False, True,
                               expected_file_ext='', **get_kwargs)

                # Check request headers
                req_headers = m.request_history[0].headers
                auth_header = req_headers.get('Authorization')
                assert auth_header and 'Basic' in auth_header

            # Token auth
            elif '`token`' in expected_log:
                token = os.environ.get(
                    auth_config[url]['variable_names']['token'])

                # Token in query string of url
                if auth_config[url].get('token_location') == 'url':
                    token_qs = urlencode({'token': token})
                    m.get(urljoin(url, token_qs), content=file_content)
                    _test_get_file(url, tmpdir, True, False, True,
                                   expected_file_ext='', **get_kwargs)
                    assert token_qs in m.request_history[0].url

                # Token in Authorization header
                else:
                    _test_get_file(url, tmpdir, True, False, True,
                                   expected_file_ext='', **get_kwargs)
                    req_headers = m.request_history[0].headers
                    auth_header = req_headers.get('Authorization')
                    assert auth_header and f'Token {token}' in auth_header

            # Oauth2 auth
            elif '`oauth2`' in expected_log:
                OAuth2Mocker().create_service_token_mock(m)
                _test_get_file(url, tmpdir, True, False, True,
                               expected_file_ext='', **get_kwargs)
            else:
                _test_get_file(url, tmpdir, True, False, True,
                               expected_file_ext='', **get_kwargs)

            assert expected_log in caplog.text


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist',
                         TEST_PARAMS
                         )
def test_get_local_file(storage_dir, use_storage_dir, cleanup_at_exit,
                        should_file_exist):
    """
    Test file retrieval of local file
    """
    url = 'file://' + TEST_FILE_PATH
    _test_get_file(url, storage_dir, use_storage_dir, cleanup_at_exit,
                   should_file_exist)


@pytest.mark.parametrize('url', [
    'badprotocol://test',
    's3:/',
    'blah'
])
def test_invalid_urls(url):
    """
    Test bad url
    """
    with pytest.raises(LookupError) as e:
        try:
            FileRetriever().get(url)
        except LookupError as e:
            assert f"In URL: {url}" in str(e)
            assert "Invalid protocol:" in str(e)
            raise


def _test_get_file(url, storage_dir, use_storage_dir, cleanup_at_exit,
                   should_file_exist, expected_file_ext=None, **get_kwargs):
    """
    Test file retrieval
    """
    if expected_file_ext is None:
        expected_file_ext = os.path.splitext(url)[-1]

    if not use_storage_dir:
        storage_dir = None

    with open(TEST_FILE_PATH, "rb") as tf:
        fr = FileRetriever(storage_dir=storage_dir,
                           cleanup_at_exit=cleanup_at_exit)
        local_copy = fr.get(url, **get_kwargs)

        assert local_copy.original_name.endswith(expected_file_ext)

        assert(tf.read() == local_copy.read())
        _assert_file(os.path.realpath(fr.storage_dir), local_copy.name)

        # Test file existence after all refs to file obj are closed/destroyed
        filepath = local_copy.name
        del fr
        del local_copy
        assert os.path.isfile(filepath) == should_file_exist


def _assert_file(storage_dir, file_path):
    """
    Assert that storage dir and file path share same parent dir
    Assert that a file exists at file_path
    """
    assert os.path.commonpath([
        storage_dir,
        file_path
    ]) == storage_dir

    assert os.path.isfile(file_path)
