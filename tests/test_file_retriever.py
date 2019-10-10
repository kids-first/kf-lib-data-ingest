import base64
import importlib
import logging
import os
from urllib.parse import quote, urlencode, urljoin

import boto3
import pytest
import requests_mock
from moto import mock_s3
from requests.auth import HTTPBasicAuth

from conftest import (
    TEST_AUTH0_AUD,
    TEST_AUTH0_DOMAIN,
    TEST_CLIENT_ID,
    TEST_CLIENT_SECRET,
    TEST_DATA_DIR,
)
from kf_lib_data_ingest.common.file_retriever import FileRetriever
from mocks import OAuth2Mocker

TEST_S3_BUCKET = "s3_bucket"
TEST_S3_PREFIX = "mock_folder"
TEST_FILENAME = "data.csv"
TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, TEST_FILENAME)
FUNNY_FILENAME = "funny name file €א.txt"
FUNNY_FILE_PATH = os.path.join(TEST_DATA_DIR, FUNNY_FILENAME)
TEST_PARAMS = [
    (False, True, False),
    (False, False, False),
    (True, False, True),
    (True, True, False),
]


@pytest.fixture(scope="session")
def storage_dir(tmpdir_factory):
    """
    Uses pytest tmpdir_factory fixture for temporarily storing
    file getter files. Used for duration of these tests and then
    deleted after tests complete
    """
    return tmpdir_factory.mktemp("retrieved_files")


@pytest.fixture(scope="function")
def s3_file():
    """
    Mock bucket creation, file upload. Return path to file on s3.
    """
    mock_s3().start()
    # Create bucket
    client = boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id="fake_access_key",
        aws_secret_access_key="fake_secret_key",
    )
    client.create_bucket(Bucket=TEST_S3_BUCKET)

    # Upload file
    client.upload_file(
        Filename=TEST_FILE_PATH,
        Bucket=TEST_S3_BUCKET,
        Key=f"{TEST_S3_PREFIX}/{TEST_FILENAME}",
    )
    # Sanity check - bucket exists
    response = client.head_bucket(Bucket=TEST_S3_BUCKET)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    yield f"s3://{TEST_S3_BUCKET}/{TEST_S3_PREFIX}/{TEST_FILENAME}"

    # Teardown
    mock_s3().stop()


@pytest.fixture(scope="function")
def auth_configs():
    os.environ["FOO_SERVICE_UNAME"] = "username"
    os.environ["FOO_SERVICE_PW"] = "password"
    os.environ["TEST_AUTH0_DOMAIN"] = TEST_AUTH0_DOMAIN
    os.environ["TEST_AUTH0_AUD"] = TEST_AUTH0_AUD
    os.environ["TEST_CLIENT_ID"] = TEST_CLIENT_ID
    os.environ["TEST_CLIENT_SECRET"] = TEST_CLIENT_SECRET
    os.environ["TEST_API_TOKEN"] = "a developer token"

    auth_configs = {
        "https://api.com/foo": {
            "type": "basic",
            "username": os.environ.get("FOO_SERVICE_UNAME"),
            "password": os.environ.get("FOO_SERVICE_PW"),
        },
        "https://test-api.kids-first.io/files/download": {
            "type": "oauth2",
            "provider_domain": os.environ.get("TEST_AUTH0_DOMAIN"),
            "audience": os.environ.get("TEST_AUTH0_AUD"),
            "client_id": os.environ.get("TEST_CLIENT_ID"),
            "client_secret": os.environ.get("TEST_CLIENT_SECRET"),
        },
        "https://kf-study-creator.kidsfirstdrc.org/download/study": {
            "type": "token",
            "token_location": "url",
            "token": os.environ.get("TEST_API_TOKEN"),
        },
        "https://api.com/download/study": {
            "type": "token",
            "token_location": "header",
            "token": os.environ.get("TEST_API_TOKEN"),
        },
        "https://api.com/bar": {
            "type": "token",
            "token": os.environ.get("TEST_API_TOKEN"),
        },
    }

    return auth_configs


@pytest.mark.parametrize(
    "use_storage_dir,cleanup_at_exit,should_file_exist", TEST_PARAMS
)
def test_s3_file(
    s3_file, storage_dir, use_storage_dir, cleanup_at_exit, should_file_exist
):
    """
    Test file retrieval via s3
    """
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "foobar_key")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "foobar_secret")
    _test_get_file(
        s3_file,
        storage_dir,
        use_storage_dir,
        cleanup_at_exit,
        should_file_exist,
    )


@pytest.mark.parametrize(
    "use_storage_dir,cleanup_at_exit,should_file_exist", TEST_PARAMS
)
def test_get_web(
    caplog, storage_dir, use_storage_dir, cleanup_at_exit, should_file_exist
):
    """
    Test file retrieval via http
    """
    # With Content-Disposition header
    url = f"http://{TEST_FILENAME}/download"
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get(
                url,
                content=tf.read(),
                headers={
                    "Content-Disposition": f"attachment; filename={TEST_FILENAME}"
                },
            )
            _test_get_file(
                url,
                storage_dir,
                use_storage_dir,
                cleanup_at_exit,
                should_file_exist,
                expected_file_name=TEST_FILENAME,
            )

    # With extended-attr Content-Disposition header
    quoted_name = quote(FUNNY_FILENAME)
    url = f"http://{quoted_name}/download"
    with open(FUNNY_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get(
                url,
                content=tf.read(),
                headers={
                    "Content-Disposition": f"attachment; filename*=utf-8''{quoted_name}"
                },
            )
            _test_get_file(
                url,
                storage_dir,
                use_storage_dir,
                cleanup_at_exit,
                should_file_exist,
                expected_file_name=FUNNY_FILENAME,
                test_file_path=FUNNY_FILE_PATH,
            )

    # Without Content-Disposition header
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get(url, content=tf.read())
            _test_get_file(
                url,
                storage_dir,
                use_storage_dir,
                cleanup_at_exit,
                should_file_exist,
                expected_file_name="download",
            )
    assert f"{url}" in caplog.text
    assert "Content-Disposition" in caplog.text


@pytest.mark.parametrize(
    "url, auth_type, expected_log",
    [
        ("http://www.example.com", None, ""),
        (
            "https://another-api.com/files",
            "not found",
            "Authentication scheme not found",
        ),
        ("https://api.com/foo/1", "basic", "Selected `basic` authentication"),
        (
            "https://kf-study-creator.kidsfirstdrc.org/download/study",
            "token",
            "Selected `token` authentication",
        ),
        ("https://api.com/bar", "token", "Selected `token` authentication"),
        (
            "https://test-api.kids-first.io/files/download/file_id",
            "oauth2",
            "Selected `oauth2` authentication",
        ),
    ],
)
def test_get_web_w_auth(
    caplog, tmpdir, auth_configs, url, auth_type, expected_log
):

    caplog.set_level(logging.INFO)
    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            file_content = tf.read()
            m.get(url, content=file_content)

            # Basic auth
            if auth_type == "basic":
                _test_get_file(
                    url, tmpdir, True, False, True, auth_configs=auth_configs
                )

                # Check request headers
                req_headers = m.request_history[0].headers
                auth_header = req_headers.get("Authorization")
                assert auth_header and "Basic" in auth_header
                username, password = base64.b64decode(
                    auth_header.split(" ")[1]
                ).split(b":")
                assert (username == b"username") and (password == b"password")

            # Token auth
            elif auth_type == "token":
                token = auth_configs[url]["token"]

                # Token in query string of url
                if auth_configs[url].get("token_location") == "url":
                    token_qs = urlencode({"token": token})
                    m.get(urljoin(url, token_qs), content=file_content)
                    _test_get_file(
                        url,
                        tmpdir,
                        True,
                        False,
                        True,
                        auth_configs=auth_configs,
                    )
                    assert token_qs in m.request_history[0].url

                # Token in Authorization header
                else:
                    _test_get_file(
                        url,
                        tmpdir,
                        True,
                        False,
                        True,
                        auth_configs=auth_configs,
                    )
                    req_headers = m.request_history[0].headers
                    auth_header = req_headers.get("Authorization")
                    assert auth_header
                    assert f"Token {token}" in auth_header

            # Oauth2 auth
            elif auth_type == "oauth2":
                OAuth2Mocker().create_service_token_mock(m)
                _test_get_file(
                    url, tmpdir, True, False, True, auth_configs=auth_configs
                )

            # Auth scheme expected but not found
            elif auth_type == "not found":
                _test_get_file(
                    url, tmpdir, True, False, True, auth_configs=auth_configs
                )
            # No auth required
            else:
                _test_get_file(url, tmpdir, True, False, True)

            assert expected_log in caplog.text


@pytest.mark.parametrize(
    "auth_config, expected_exc",
    [
        ("foo", TypeError),
        ({"something": {}}, AssertionError),
        ({"type": "auth", "foo": "bar"}, None),
    ],
)
def test_validate_auth_configs(auth_config, expected_exc):

    fr = FileRetriever(cleanup_at_exit=True)
    if expected_exc:
        with pytest.raises(expected_exc) as e:
            fr._validate_auth_config(auth_config)
    else:
        fr._validate_auth_config(auth_config)


@pytest.mark.parametrize(
    "use_storage_dir,cleanup_at_exit,should_file_exist", TEST_PARAMS
)
def test_get_local_file(
    storage_dir, use_storage_dir, cleanup_at_exit, should_file_exist
):
    """
    Test file retrieval of local file
    """
    url = "file://" + TEST_FILE_PATH
    _test_get_file(
        url, storage_dir, use_storage_dir, cleanup_at_exit, should_file_exist
    )


@pytest.mark.parametrize("url", ["badprotocol://test", "s3:/", "blah"])
def test_invalid_urls(url):
    """
    Test bad url
    """
    with pytest.raises(LookupError) as e:
        FileRetriever().get(url)
    assert f"In URL: {url}" in str(e.value)
    assert "Invalid protocol:" in str(e.value)


def _test_get_file(
    url,
    storage_dir,
    use_storage_dir,
    cleanup_at_exit,
    should_file_exist,
    expected_file_name=None,
    auth_configs=None,
    test_file_path=TEST_FILE_PATH,
):
    """
    Test file retrieval
    """
    if not use_storage_dir:
        storage_dir = None

    with open(test_file_path, "rb") as tf:
        fr = FileRetriever(
            storage_dir=storage_dir,
            cleanup_at_exit=cleanup_at_exit,
            auth_configs=auth_configs,
        )
        local_copy = fr.get(url)

        if expected_file_name:
            assert expected_file_name == local_copy.original_name

        assert tf.read() == local_copy.read()
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
    assert os.path.commonpath([storage_dir, file_path]) == storage_dir

    assert os.path.isfile(file_path)


def test_static_auth_configs():
    FileRetriever.static_auth_configs = {"test": "test"}

    # test persists locally
    fr = FileRetriever()
    assert fr.static_auth_configs == {"test": "test"}

    # test persists across modules
    test_spec = importlib.util.spec_from_loader("test_module", loader=None)
    test_module = importlib.util.module_from_spec(test_spec)
    test_code = (
        "def test_static_auth_configs():\n"
        " from kf_lib_data_ingest.common.file_retriever import FileRetriever\n"
        " fr = FileRetriever()\n"
        ' assert fr.static_auth_configs == {"test": "test"}\n'
    )
    exec(test_code, test_module.__dict__)
    test_module.test_static_auth_configs()
    fr.static_auth_configs = None
