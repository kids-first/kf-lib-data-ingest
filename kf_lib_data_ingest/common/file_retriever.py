"""
FileRetriever is a class for downloading the contents of remote files using
whatever mechanism is appropriate for the given protocol.
"""

import logging
import os
import shutil
import tempfile
from urllib.parse import (
    urlparse,
    urlencode
)

import boto3
import botocore
from requests.auth import HTTPBasicAuth

from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type
)
from kf_lib_data_ingest.network import (
    utils,
    oauth2
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)

PROTOCOL_SEP = "://"


def split_protocol(url):
    """
    Split file url string into protocol and path.

    Expected format is always <protocol>://<path to file>, even for local
    files.

    :param url: URL of local or remote file
    :type url: string
    """
    split_url = url.split(PROTOCOL_SEP, 1)
    protocol, path = None, None
    if len(split_url) == 2:
        protocol, path = split_url[0], split_url[1]

    if protocol not in FileRetriever._getters:
        raise LookupError(
            f"In URL: {url}\n"
            f"Invalid protocol: {protocol}\n"
            f"Options are: {[x+'://' for x in FileRetriever._getters]}"
        )
    return protocol, path


def _s3_save(protocol, source_loc, dest_obj, auth=None, auth_config=None,
             logger=None):
    """
    Get contents of a file from Amazon S3 to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth: optional S3 auth profile (defaults to all available profiles)
    :type auth: str
    :param auth_config: optional dict mapping URL patterns to authentication
    schemes and environment variables containing necessary auth parameters
    :type auth_config: dict

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)
    if auth:
        aws_profiles = auth
    else:
        aws_profiles = boto3.Session().available_profiles + [None]

    bucket, key = source_loc.split("/", 1)

    for profile in aws_profiles:
        try:
            logger.info("S3 download - Trying auth profile '%s'", profile)
            s3 = boto3.session.Session(profile_name=profile).resource("s3")
            s3.Object(bucket, key).download_fileobj(dest_obj)
            return
        except (
            # HACK: ClientError is too generic but (I think) all we get for now
            # See https://github.com/boto/botocore/issues/1400
            botocore.exceptions.ClientError,
            botocore.exceptions.NoCredentialsError
        ):
            pass

    raise botocore.exceptions.NoCredentialsError()  # never got the file


def _web_save(protocol, source_loc, dest_obj, auth=None, auth_config=None,
              logger=None):
    """
    Get contents of a file from a web server to a local file-like object.

    Preserves the name of the original file if Content-Disposition
    header is provided in the response.
    See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition # noqa E501

    Authentication
    --------------
    Currently supports the following authentication schemes:
        - HTTP Basic Authentication
        - Token Authentication
        - OAuth 2 Authentication

    If `auth_config` is provided, inspect the URL to select the authentication
    scheme and its configuration dict from `auth_config`

    If `auth` is provided, use that to get the resource - uses HTTP basic auth

    If neither `auth` nor `auth_config` are provided then assume the resource
    requires no auth. Just send a get request to fetch it

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth: optional requests-compatible auth object
    :type auth: http://docs.python-requests.org/en/master/user/authentication/
    :param auth_config: optional dict mapping URL patterns to authentication
    schemes and environment variables containing necessary auth parameters
    :type auth_config: dict

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)
    url = protocol + PROTOCOL_SEP + source_loc

    # Fetch a protected file using auth scheme determined by URL inspection
    # Use auth parameters from auth config for the selected auth scheme
    if auth_config:
        auth_scheme_params = _select_auth_scheme(url, auth_config)

        # If no auth config for URL, fallback to fetch an unprotected file
        if not auth_scheme_params:
            logger.info(f'Authentication scheme not found for url {url}')
            utils.get_file(url, dest_obj)
            return

        auth_scheme = auth_scheme_params['type']
        var_names = auth_scheme_params['variable_names']
        logger.info(f'Selected `{auth_scheme}` authentication to fetch {url}')

        # Basic auth
        if auth_scheme == 'basic':
            auth = HTTPBasicAuth(os.environ.get(var_names.get('username')),
                                 os.environ.get(var_names.get('password')))
            utils.get_file(url, dest_obj, auth=auth)

        # Token auth
        if auth_scheme == 'token':
            token = os.environ.get(var_names.get('token'))

            if auth_scheme_params.get('token_location', 'header') == 'url':
                utils.get_file(
                    f"{url}?{urlencode({'token': token})}", dest_obj)
            else:
                utils.get_file(
                    url, dest_obj, headers={'Authorization': f'Token {token}'})

        # OAuth 2
        elif auth_scheme == 'oauth2':
            kwargs = {
                var_name: os.environ.get(var_names.get(var_name))
                for var_name in ['provider_domain',
                                 'audience',
                                 'client_id',
                                 'client_secret']
            }
            oauth2.get_file(url, dest_obj, **kwargs)

    else:
        # Fetch protected file using auth from kwargs
        if auth:
            logger.info(f'Using `basic` authentication scheme to fetch {url}')
            utils.get_file(url, dest_obj, auth=auth)
        # Fetch unprotected file
        else:
            utils.get_file(url, dest_obj)


def _file_save(protocol, source_loc, dest_obj, auth=None, auth_config=None,
               logger=None):
    """
    Get contents of a file from a local file path to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: receives the data downloaded from the source file
    :type dest_obj: file-like object
    :param auth: do not use
    :type auth: None
    :param auth_config: do not use
    :type auth_config: None

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)
    with open(source_loc, "rb") as orig:
        shutil.copyfileobj(orig, dest_obj)


def _select_auth_scheme(url, auth_config):
    """
    Select authentication scheme by inspecting `url`. If `url` starts with
    any of the keys in auth_config, return the value, a dict containing
    necessary parameters for the selected auth scheme, of the first key match.

    See FileRetriever._validate_auth_config for details on auth_config format

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


class FileRetriever(object):
    """
    A self-cleaning file contents downloader. Downloads contents of remote
    files to local temp files. When the FileRetriever instance loses scope, the
    temp files optionally go away.
    """
    _getters = {
        "s3": _s3_save,
        "http": _web_save,
        "https": _web_save,
        "file": _file_save
    }

    def __init__(self, storage_dir=None, cleanup_at_exit=True,
                 auth_config=None):
        """
        :param storage_dir: optional specific tempfile storage location
        :type storage_dir: str
        :param cleanup_at_exit: should the temp files vanish at exit
        :type cleanup_at_exit: bool
        :param auth_config: optional dict mapping URL patterns to
        authentication schemes and environment variables containing necessary
        auth parameters
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.cleanup_at_exit = cleanup_at_exit
        if storage_dir:
            self.__tmpdir = None
            self.storage_dir = storage_dir
        else:
            self.__tmpdir = tempfile.TemporaryDirectory(prefix=".ingest_tmp_",
                                                        dir=".")
            self.storage_dir = self.__tmpdir.name
        self._files = {}
        self.auth_config = auth_config

    def get(self, url, auth=None):
        """
        Retrieve the contents of a remote file.

        :param url: full file URL
        :type url: str
        :param auth: auth argument appropriate to type
        :type auth: various

        :raises LookupError: url is not of one of the handled protocols
        :returns: a file-like object containing the remote file contents
        """
        self.logger.info("Fetching %s with primary auth '%s'", url, auth)
        protocol, path = split_protocol(url)

        self.logger.info(
            "Detected protocol '%s' --> Using getter %s",
            protocol, FileRetriever._getters[protocol].__name__
        )

        # TODO: Either remove this try wrapper or remove this message.
        # I think there may be a better way to handle exceptional cleanup. -Avi
        try:
            if url not in self._files:
                # Temporary file object that optionally deletes itself at the
                # end of FileRetriever instance scope (if the enclosing folder
                # isn't already being deleted)
                self._files[url] = tempfile.NamedTemporaryFile(
                    dir=self.storage_dir,
                    delete=self.cleanup_at_exit and not self.__tmpdir
                )

                # Set auth config if there is one and validate it
                if self.auth_config:
                    self._validate_auth_config(self.auth_config)

                # Fetch the remote data
                kwargs = {
                    'auth': auth,
                    'logger': self.logger,
                    'auth_config': self.auth_config
                }
                FileRetriever._getters[protocol](
                    protocol, path, self._files[url],
                    **kwargs
                )
                if not hasattr(self._files[url], 'original_name'):
                    filename = urlparse(url).path.rsplit('/', 1)[-1]
                    self._files[url].original_name = filename

            self._files[url].seek(0)
            return self._files[url]
        except Exception as e:
            if self.__tmpdir:
                self.__tmpdir.cleanup()
            raise e

    def _validate_auth_config(self, auth_config):
        """
        Validate config dict containing authentication parameters

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
        """
        # Dict with str keys
        assert_safe_type(auth_config, dict)
        assert_all_safe_type(list(auth_config.keys()), str)

        # All sub dicts have type and variable_names w correct types
        for url, cfg in auth_config.items():
            auth_scheme = cfg.get('type')
            assert auth_scheme, ('A dict in Auth config must have `type` key'
                                 ' to define the authentication scheme')
            assert_safe_type(auth_scheme, str)
            var_names = cfg.get('variable_names')
            assert var_names, ('A dict in auth config must have'
                               ' `variable_names` to define required auth'
                               ' parameters')
            assert_safe_type(var_names, dict)
