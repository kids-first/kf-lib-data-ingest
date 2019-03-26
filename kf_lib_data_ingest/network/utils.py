"""
Common network (HTTP, TCP, whatever) related functionality
"""
import cgi
import logging

import requests

logger = logging.getLogger(__name__)


def get_file(url, dest_obj, **kwargs):
    """
    Get the file at `url` and write to `dest_obj`, a file-like obj

    :param url: the URL to send the GET request to
    :type url: str
    :param dest_obj: a file-like object that receives the data downloaded
    :type dest_obj: a file-like object
    :param kwargs: keyword args forwarded to requests.get
    :type kwargs: dict
    :returns response: requests.Response object
    """

    kwargs['stream'] = True
    response = requests.get(url, **kwargs)

    if response.status_code == 200:
        # Get filename from Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition', '')
        _, cdisp_params = cgi.parse_header(content_disposition)
        filename = cdisp_params.get('filename*')
        # RFC 5987 ext-parameter is actually more complicated than this,
        # but this should get us 90% there, and the rfc6266 python lib is
        # broken after PEP 479. *sigh* - Avi K
        if filename and filename.lower.startswith('utf-8'):
            filename = filename.split("'", 2)[2].decode('utf-8')
        else:
            filename = cdisp_params.get('filename')

        if filename:
            dest_obj.original_name = filename
        else:
            # Header did not provide filename
            logging.warning(f'{url} returned unhelpful or missing '
                            'Content-Disposition header '
                            f'{content_disposition}. '
                            'HTTP(S) file responses should include a '
                            'Content-Disposition header specifying '
                            'filename.')

        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                dest_obj.write(chunk)
        dest_obj.seek(0)
        response.close()

    else:
        logger.warning(f'Could not fetch {url}. Caused by '
                       f'{response.text}')

    return response


def select_auth_scheme(url, auth_config):
    """
    Select authentication scheme by inspecting `url`. If `url` starts with
    any of the keys in auth_config, return the value of the first key match.

    Authentication schemes along with their required parameters are stored in
    `auth_config`.

    `auth_config` looks something like:

        {
            'http://api.com/files': {
                'type': 'basic',
                'variable_names': {
                    'username': 'MY_FILES_API_UNAME',
                    'password': 'MY_FILES_API_PW'
                }
            },
            'https://kf-api-study-creator.kids-first.io/download': {
                'type': 'oauth2',
                'variable_names': {
                    'client_id': 'STUDY_CREATOR_CLIENT_ID',
                    'client_secret': 'STUDY_CREATOR_CLIENT_ID',
                    'provider_domain': 'STUDY_CREATOR_AUTH0_DOMAIN',
                    'audience': 'STUDY_CREATOR_API_IDENTIFIER',
                }
            }
        }


    `type` is the authentication scheme

    `variable_names` is a key value list where a key is a required auth
    parameter for the authentication scheme and the value is the name of an
    environment variable where the parameter value is stored.

    :param url: the URL to be inspected
    :type url: str
    :param auth_config: configuration dict of auth schemes and parameters
    :type auth_config: dict

    :returns selected_cfg: configuration dict of the selected auth scheme
    """
    selected_cfg = None
    if auth_config:
        for key, cfg in auth_config.items():
            if url.startswith(key):
                selected_cfg = cfg
                break
    return selected_cfg
