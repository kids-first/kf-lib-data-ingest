import os
import pytest
import requests_mock
from common.file_retriever import FileRetriever
from conftest import TEST_DATA_DIR

FR_tmp = FileRetriever()
FR_local = FileRetriever(storage_dir=None, cleanup_at_exit=True)


def test_get_unknown():
    with pytest.raises(LookupError):
        FR_tmp.get('badprotocol://test')


def test_get_web():
    with open(os.path.join(TEST_DATA_DIR, "yaml_schema.yml"), "rb") as tf:
        with requests_mock.Mocker() as m:
            m.get('http://test', content=tf.read())
            local_copy = FR_tmp.get('http://test')

        tf.seek(0)
        assert(tf.read() == local_copy.read())
        assert_file(os.path.realpath(FR_tmp.storage_dir), local_copy.name)


def assert_file(storage_dir, file_path):
    assert os.path.commonpath([
        os.getcwd(), storage_dir
    ]) == os.getcwd()

    assert os.path.commonpath([
        storage_dir,
        file_path
    ]) == storage_dir

    assert os.path.isfile(file_path)


def test_get_file():
    path = os.path.join(TEST_DATA_DIR, "yaml_schema.yml")
    with open(path, "rb") as tf:
        local_copy = FR_tmp.get('file://' + path)

        tf.seek(0)
        assert(tf.read() == local_copy.read())

        assert_file(os.path.realpath(FR_tmp.storage_dir), local_copy.name)
