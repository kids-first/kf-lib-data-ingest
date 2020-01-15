import requests_mock

from conftest import (
    TEST_AUTH0_AUD,
    TEST_AUTH0_DOMAIN,
    TEST_CLIENT_ID,
    TEST_CLIENT_SECRET,
)
from kf_lib_data_ingest.network import oauth2


class OAuth2Mocker(object):
    """
    Mock out kf_lib_data_ingest.network.oauth2
    """

    def get_service_token(
        self,
        provider_domain=TEST_AUTH0_DOMAIN,
        audience=TEST_AUTH0_AUD,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
        **mock_kwargs,
    ):

        with requests_mock.Mocker() as m:
            self.create_service_token_mock(
                m,
                provider_domain,
                audience,
                client_id,
                client_secret,
                **mock_kwargs,
            )
            token = oauth2.get_service_token(
                provider_domain, audience, client_id, client_secret
            )
            if mock_kwargs.get("expected_status"):
                assert m.call_count == 1

            return token

    def get_file(
        self,
        url,
        dest_obj,
        provider_domain=TEST_AUTH0_DOMAIN,
        audience=TEST_AUTH0_AUD,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
        **mock_kwargs,
    ):

        with requests_mock.Mocker() as m:
            token_kwargs = {
                "provider_domain": provider_domain,
                "audience": audience,
                "client_id": client_id,
                "client_secret": client_secret,
            }
            self.create_get_file_mock(
                m, url, dest_obj, **token_kwargs, **mock_kwargs
            )
            response = oauth2.get_file(url, dest_obj, **token_kwargs)

            if mock_kwargs.get("expected_status"):
                assert m.call_count == 2

            return response

    def create_get_file_mock(
        self,
        m,
        url,
        dest_obj,
        provider_domain=TEST_AUTH0_DOMAIN,
        audience=TEST_AUTH0_AUD,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
        **mock_kwargs,
    ):

        self.create_service_token_mock(
            m,
            provider_domain,
            audience,
            client_id,
            client_secret,
            status_code=200,
        )
        m.get(url, **mock_kwargs)

    def create_service_token_mock(
        self,
        m,
        provider_domain=TEST_AUTH0_DOMAIN,
        audience=TEST_AUTH0_AUD,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
        **mock_kwargs,
    ):

        if not mock_kwargs.get("json"):
            expected_status = mock_kwargs.get("status_code", 200)
            if expected_status == 200:
                mock_kwargs["json"] = {
                    "access_token": "the token",
                    "expires_in": 5,
                    "scope": "download:file",
                    "token_type": "Bearer",
                }
            else:
                mock_kwargs["json"] = {
                    "error": "some error",
                    "error_description": "reason for error",
                }

        m.post(f"https://{provider_domain}/oauth/token", **mock_kwargs)
