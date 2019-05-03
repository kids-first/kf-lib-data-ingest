import logging

import pytest
import requests_mock

from kf_lib_data_ingest.config import VERSION, PROJECT_GITHUB_URL
from kf_lib_data_ingest.network.utils import get_tag_commit_sha


def test_get_tag_commit_sha(caplog):
    """
    Test get_tag_commit_sha
    """
    caplog.set_level(logging.DEBUG)

    # Test successful get
    assert get_tag_commit_sha(VERSION)
    assert 'Got tags' in caplog.text

    # Test get tag that doesn't exist
    with pytest.raises(Exception):
        try:
            get_tag_commit_sha(VERSION + 1)
        except Exception as e:
            assert 'Could not find tag' in caplog.text
            raise e

    # Test get tag when status_code != 200
    with requests_mock.Mocker(real_http=True) as m:
        endpoint = f'{PROJECT_GITHUB_URL}/tags'
        m.get(endpoint, status_code=400)
        with pytest.raises(Exception):
            try:
                get_tag_commit_sha('foo')
            except Exception as e:
                assert 'Could not fetch tags' in caplog.text
                raise e
