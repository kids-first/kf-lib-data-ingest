import os
from pprint import pprint

import pytest
import requests_mock

import boto3
from moto import mock_s3

from common.file_retriever import FileRetriever
from conftest import TEST_DATA_DIR

TEST_S3_BUCKET = "s3_bucket"
TEST_S3_PREFIX = "mock_folder"
TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, 'data.csv')


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
    Mock bucket creation, file upload
    Return path to file on s3
    """
    mock_s3().start()
    # Create bucket
    client = boto3.client("s3",
                          region_name="eu-west-1",
                          aws_access_key_id="fake_access_key",
                          aws_secret_access_key="fake_secret_key")
    client.create_bucket(Bucket=TEST_S3_BUCKET)

    # Upload file
    filename = 'data.csv'
    s3_path = (f's3://{TEST_S3_BUCKET}/{TEST_S3_PREFIX}/'
               f'{filename}')
    client.upload_file(Filename=TEST_FILE_PATH,
                       Bucket=TEST_S3_BUCKET,
                       Key=f'{TEST_S3_PREFIX}/{filename}')
    # Sanity check - bucket exists
    response = client.head_bucket(Bucket=TEST_S3_BUCKET)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    yield s3_path

    # Teardown
    mock_s3().stop()


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist', [
    (False, True, False),
    (False, False, False),
    (True, False, True),
    (True, True, False)
])
def test_s3_file(s3_file, storage_dir, use_storage_dir, cleanup_at_exit,
                 should_file_exist):
    """
    Test file retrieval via s3
    """
    if not use_storage_dir:
        storage_dir = None

    with open(TEST_FILE_PATH, "rb") as tf:
        fr = FileRetriever(storage_dir=storage_dir,
                           cleanup_at_exit=cleanup_at_exit)
        local_copy = fr.get(s3_file, auth=None)

        tf.seek(0)
        assert(tf.read() == local_copy.read())
        assert_file(os.path.realpath(fr.storage_dir), local_copy.name)

    # Test file existence after all refs to file obj are closed/destroyed
    filepath = local_copy.name
    del fr
    del local_copy
    assert os.path.isfile(filepath) == should_file_exist


@pytest.mark.parametrize('url', [
    'badprotocol://test',
    's3:/',
    'blah'
])
def test_get_unknown(url):
    """
    Test bad url
    """
    with pytest.raises(LookupError) as e:
        FileRetriever().get(url)
        assert f"Retrieving URL: {url} " in e
        assert "No client found for protocol:" in e


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist', [
    (False, True, False),
    (False, False, False),
    (True, False, True),
    (True, True, False)
])
def test_get_web(storage_dir, use_storage_dir, cleanup_at_exit,
                 should_file_exist):
    """
    Test file retrieval via http
    """
    if not use_storage_dir:
        storage_dir = None

    with open(TEST_FILE_PATH, "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get('http://test', content=tf.read())
            fr = FileRetriever(storage_dir=storage_dir,
                               cleanup_at_exit=cleanup_at_exit)
            local_copy = fr.get('http://test')

        tf.seek(0)
        assert(tf.read() == local_copy.read())
        assert_file(os.path.realpath(fr.storage_dir), local_copy.name)

    # Test file existence after all refs to file obj are closed/destroyed
    filepath = local_copy.name
    del fr
    del local_copy
    assert os.path.isfile(filepath) == should_file_exist


@pytest.mark.parametrize('use_storage_dir,cleanup_at_exit,should_file_exist', [
    (False, True, False),
    (False, False, False),
    (True, False, True),
    (True, True, False)
])
def test_get_file(storage_dir, use_storage_dir, cleanup_at_exit,
                  should_file_exist):
    """
    Test file retrieval from local machine
    """
    if not use_storage_dir:
        storage_dir = None

    url = 'file://' + TEST_FILE_PATH

    # Open local file
    with open(TEST_FILE_PATH, "rb") as tf:
        fr = FileRetriever(storage_dir=storage_dir,
                           cleanup_at_exit=cleanup_at_exit)
        # Get local file
        local_copy = fr.get(url)

        # Test contents are as expected
        tf.seek(0)
        assert(tf.read() == local_copy.read())

        # Test file exists in temp location
        assert_file(os.path.realpath(fr.storage_dir), local_copy.name)

    # Test file existence after all refs to file obj are closed/destroyed
    filepath = local_copy.name
    del fr
    del local_copy
    assert os.path.isfile(filepath) == should_file_exist


def assert_file(storage_dir, file_path):
    """
    Assert that storage dir and file path share same parent dir
    Assert that a file exists at file_path
    """
    assert os.path.commonpath([
        storage_dir,
        file_path
    ]) == storage_dir

    assert os.path.isfile(file_path)
