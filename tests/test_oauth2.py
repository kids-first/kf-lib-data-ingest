import os
import logging
import tempfile

import pytest
import requests_mock

from kf_lib_data_ingest.network import oauth2
from conftest import (
    TEST_AUTHO0_DOMAIN,
    TEST_AUTH0_AUD,
    TEST_CLIENT_ID,
    TEST_CLIENT_SECRET,
    TEST_DATA_DIR
)
TEST_FILE_PATH = os.path.join(TEST_DATA_DIR, 'data.csv')


@pytest.fixture(scope='function')
def info_caplog(caplog):
    caplog.set_level(logging.INFO)
    return caplog


@pytest.mark.parametrize(
    'provider_domain, audience, client_id, client_secret, expected_status, expected_log',
    [(TEST_AUTHO0_DOMAIN, TEST_AUTH0_AUD, TEST_CLIENT_ID, TEST_CLIENT_SECRET,
      200, 'Successfully fetched token'),
     (TEST_AUTHO0_DOMAIN, TEST_AUTH0_AUD, '', '',
      None, 'Client ID and secret are required'),
     (TEST_AUTHO0_DOMAIN, TEST_AUTH0_AUD, None, None,
      None, 'Client ID and secret are required'),
     (TEST_AUTHO0_DOMAIN, 'foo', TEST_CLIENT_ID, TEST_CLIENT_SECRET,
      403, 'Could not fetch access token')
     ])
def test_get_service_token(info_caplog, provider_domain, audience, client_id,
                           client_secret, expected_status, expected_log):
    """
    Test oauth2.get_service_token

    1 - All valid inputs
    2, 3 - Missing client_id and client_secret
    4 - Wrong API Audience, Access denied
    """
    token = oauth2.get_service_token(provider_domain,
                                     audience,
                                     client_id,
                                     client_secret)
    if expected_status == 200:
        assert token
    else:
        assert not token
    assert expected_log in info_caplog.text


def test_bad_token_response(info_caplog):
    """
    Test bad token response. All inputs to oauth2.get_service_token are
    correct but the access token is missing
    """

    with requests_mock.Mocker() as m:
        m.post(f'https://{TEST_AUTHO0_DOMAIN}/oauth/token',
               json={'expires_in': 5,
                     'scope': 'download:file',
                     'token_type': 'Bearer'})

        token = oauth2.get_service_token(TEST_AUTHO0_DOMAIN,
                                         TEST_AUTH0_AUD,
                                         TEST_CLIENT_ID,
                                         TEST_CLIENT_SECRET)
        assert not token
        assert 'Unexpected response content' in info_caplog.text


@pytest.mark.parametrize(
    'url, expected_status, expected_log',
    [
        (TEST_AUTH0_AUD, 200, 'Successfully authenticated'),
        (TEST_AUTH0_AUD, 403, 'Could not get')
    ])
@requests_mock.Mocker(kw='mock', real_http=True)
def test_get_file(info_caplog, tmpdir, url, expected_status, expected_log,
                  **kwargs):
    """
    Test oauth2.get_file()
    """
    m = kwargs['mock']
    with open(TEST_FILE_PATH, 'rb') as f:
        expected_content = f.read()
        kwargs = {
            'status_code': expected_status,
            'content': None
        }
        if expected_status == 200:
            kwargs['content'] = expected_content

        m.get(url, **kwargs)

        token_kwargs = {
            'provider_domain': TEST_AUTHO0_DOMAIN,
            'audience': TEST_AUTH0_AUD,
            'client_id': TEST_CLIENT_ID,
            'client_secret': TEST_CLIENT_SECRET
        }

        fp = os.path.join(tmpdir, 'downloaded_file')
        with open(fp, 'w+b') as dest_obj:
            response = oauth2.get_file(url, dest_obj, **token_kwargs)

            assert expected_status == response.status_code
            assert expected_log in info_caplog.text
            assert os.path.isfile(fp)
            if expected_status == 200:
                assert dest_obj.read() == expected_content
            else:
                assert not dest_obj.read()
