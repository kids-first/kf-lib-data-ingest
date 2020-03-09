"""
Module for requests using OAuth 2 based authentication/authorization
"""

import logging
from pprint import pformat

from kf_lib_data_ingest.network import utils

logger = logging.getLogger(__name__)


def get_service_token(provider_domain, audience, client_id, client_secret):
    """
    Get OAuth 2 access token for the given ``client_id``, ``client_secret``
    from the OAuth2 provider, ``provider_domain``, using the
    Client Credentials grant type

    :param provider_domain: the OAuth2 provider's domain name \
    (i.e. ``kidsfirst.auth0.org``)
    :type provider_domain: ``str``
    :param audience: the https URL of the endpoint we're trying to \
    access (i.e. https://kf-api-study-creator.kidsfirst.io/files/download)
    :type audience: ``str``
    :param client_id: the client ID for this application issued by the provider
    :type client_id: ``str``
    :param client_secret: the client secret for this application \
    issued by the provider
    :type client_secret: ``str``
    :return: the access token ``string``
    """
    token = None
    if not (client_id and client_secret):
        logger.error(
            "Client ID and secret are required to fetch an access token!"
        )
        return token

    body = {
        "audience": audience,
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    oauth_token_url = f"https://{provider_domain}/oauth/token"
    logging.info(
        f"Fetching token from {oauth_token_url} to access "
        f"{audience} resources"
    )

    response = utils.RetrySession(connect=1).post(oauth_token_url, json=body)

    if response.status_code != 200:
        logger.error(
            f"Could not fetch access token from {oauth_token_url}! "
            f"Caused by: '{response.text}'. Code: {response.status_code}"
        )

        return token

    resp_body = response.json()

    token = resp_body.pop("access_token", None)

    if not token:
        logger.error(
            f"Unexpected response content from {oauth_token_url}, "
            f"status_code: {response.status_code}"
        )
        return

    logger.info(f"Successfully fetched token,\n{pformat(resp_body)}")

    return token


def get_file(
    url,
    dest_obj,
    provider_domain=None,
    audience=None,
    client_id=None,
    client_secret=None,
    **kwargs,
):
    """
    Get an OAuth2 protected file at URL, ``url``. Forward ``kwargs`` to
    ``kf_lib_data_ingest.network.utils.http_get_file``

    Get the service token first, then fetch the resources using the token.

    :param url: the URL of the resource to fetch
    :type url: ``str``
    :param provider_domain: See ``get_service_token``
    :param audience: See ``get_service_token``
    :param client_id: See ``get_service_token``
    :param client_secret: See ``get_service_token``
    :param kwargs: keyword args that will be forwarded to \
    ``kf_lib_data_ingest.network.utils.get``
    :type kwargs: ``dict``
    :return: the ``requests.Response`` object
    :rtype: ``requests.Response``
    """
    # Get access token to request resource
    token = get_service_token(
        provider_domain, audience, client_id, client_secret
    )

    # Something went wrong with getting token
    # Error messages already logged in get_service_token, just return None
    if not token:
        return None

    # Send request with token
    headers = kwargs.pop("headers", {})
    headers.update({"Authorization": f"Bearer {token}"})
    kwargs["headers"] = headers

    # Force HTTPS for security
    if not url.lower().startswith("https://"):
        url = "https://" + url.split("://", 1)[1]

    response = utils.http_get_file(url, dest_obj, **kwargs)

    if response.status_code == 200:
        logger.info("Successfully authenticated and fetched protected file")
        return response
    else:
        logger.error(
            f"Could not get {url}! Caused by {response.text}, "
            f"status_code: {response.status_code}"
        )

    return response
