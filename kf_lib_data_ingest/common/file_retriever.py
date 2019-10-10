"""
FileRetriever is a class for downloading the contents of remote files using
whatever mechanism is appropriate for the given protocol.
"""

import logging
import shutil
import tempfile
from urllib.parse import urlencode, urlparse

import boto3
import botocore
from requests.auth import HTTPBasicAuth
from urllib3.util import retry

from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.network import oauth2, utils

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)

# hide
# urllib3.util.retry - DEBUG - Converted retries value: False -> Retry(total=False, connect=None, read=None, redirect=0, status=None)  # noqa E501
retry.log.setLevel(logging.INFO)


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


def _s3_save(protocol, source_loc, dest_obj, auth_config=None, logger=None):
    """
    Get contents of a file from Amazon S3 to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth_config: a dict of necessary auth parameters (i.e. aws_profile)
    If profile not provided, default to all available profiles
    :type auth_config: dict

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)

    if auth_config:
        aws_profiles = auth_config.get("aws_profile")
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
            botocore.exceptions.NoCredentialsError,
        ):
            pass

    raise botocore.exceptions.NoCredentialsError()  # never got the file


def _web_save(protocol, source_loc, dest_obj, auth_config=None, logger=None):
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

    If `auth_config` is provided use the auth scheme and necessary auth
    parameters in `auth_config` to get the file

    If `auth_config` is not provided then assume the file
    requires no auth. Just send a get request to fetch it

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth_config: a dict of necessary auth parameters for a particular
    authentication scheme (i.e basic, oauth2, token, etc)
    :type auth_config: dict

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)
    url = protocol + PROTOCOL_SEP + source_loc

    # Fetch a protected file using auth scheme and parameters in auth_config
    if auth_config:
        auth_scheme = auth_config["type"]

        # Basic auth
        if auth_scheme == "basic":
            auth = HTTPBasicAuth(
                auth_config.get("username"), auth_config.get("password")
            )
            utils.http_get_file(url, dest_obj, auth=auth)

        # Token auth
        if auth_scheme == "token":
            token = auth_config.get("token")

            if auth_config.get("token_location") == "url":
                utils.http_get_file(
                    f"{url}?{urlencode({'token': token})}", dest_obj
                )
            else:
                utils.http_get_file(
                    url, dest_obj, headers={"Authorization": f"Token {token}"}
                )

        # OAuth 2
        elif auth_scheme == "oauth2":
            kwargs = {
                var_name: auth_config.get(var_name)
                for var_name in [
                    "provider_domain",
                    "audience",
                    "client_id",
                    "client_secret",
                ]
            }
            oauth2.get_file(url, dest_obj, **kwargs)

    # Fetch a file with no auth
    else:
        utils.http_get_file(url, dest_obj)


def _file_save(protocol, source_loc, dest_obj, auth_config=None, logger=None):
    """
    Get contents of a file from a local file path to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: receives the data downloaded from the source file
    :type dest_obj: file-like object
    :param auth_config: do not use
    :type auth_config: None

    :returns: None, the data goes to dest_obj
    """
    logger = logger or logging.getLogger(__name__)
    with open(source_loc, "rb") as orig:
        shutil.copyfileobj(orig, dest_obj)


def _select_auth_scheme(url, auth_configs):
    """
    Select authentication scheme by inspecting `url`. If `url` starts with
    any of the keys in auth_config, return the value, a dict containing
    necessary parameters for the selected auth scheme, of the first key match.

    :param url: the URL to be inspected
    :type url: str
    :param auth_configs: optional dict mapping URL patterns to authentication
    schemes and necessary auth parameters
    :type auth_configs: dict

    :returns cfg: configuration dict of the selected auth scheme
    """
    if auth_configs:
        for key, cfg in auth_configs.items():
            if url.startswith(key):
                return cfg


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
        "file": _file_save,
    }
    static_auth_configs = None

    def __init__(
        self, storage_dir=None, cleanup_at_exit=True, auth_configs=None
    ):
        """
        Construct FileRetriever instance

        `auth_configs` looks something like:
            {
                'http://api.com/files': {
                    'type': 'basic',
                    'username': 'the username',
                    'password': 'the password'
                    }
                },
                'https://kf-api-study-creator.kids-first.io/download': {
                    'type': 'oauth2',
                    'client_id': 'STUDY_CREATOR_CLIENT_ID',
                    'client_secret': 'STUDY_CREATOR_CLIENT_ID',
                    'provider_domain': 'STUDY_CREATOR_AUTH0_DOMAIN',
                    'audience': 'STUDY_CREATOR_API_IDENTIFIER',
                },
                's3://bucket/key': {
                    'aws_profile': 'default'
                    }
                }
            }

        The `type` key specifies the authentication scheme to use when fetching
        a particular URL. Getters in FileRetriever._getters are responsible
        for interpreting particular auth config dicts in `auth_configs`.

        :param storage_dir: optional specific tempfile storage location
        :type storage_dir: str
        :param cleanup_at_exit: should the temp files vanish at exit
        :type cleanup_at_exit: bool
        :param auth_configs: optional dict mapping URL patterns to
        authentication schemes and necessary auth parameters
        :type auth_configs: dict
        """
        self.logger = logging.getLogger(type(self).__name__)
        self.cleanup_at_exit = cleanup_at_exit
        if storage_dir:
            self.__tmpdir = None
            self.storage_dir = storage_dir
        else:
            self.__tmpdir = tempfile.TemporaryDirectory(
                prefix=".ingest_tmp_", dir="."
            )
            self.storage_dir = self.__tmpdir.name
        self._files = {}
        self.auth_configs = auth_configs or FileRetriever.static_auth_configs

        if self.auth_configs:
            assert_safe_type(self.auth_configs, dict)

    def get(self, url, auth_config=None):
        """
        Retrieve the contents of a remote file.

        :param url: full file URL
        :type url: str
        :param auth_config: optional dict containing necessary authentication
         parameters needed to fetch URL
        :type auth_config: dict

        :raises LookupError: url is not of one of the handled protocols
        :returns: a file-like object containing the remote file contents
        """
        self.logger.info("Fetching %s", url)
        protocol, path = split_protocol(url)

        self.logger.info(
            "Detected protocol '%s' --> Using getter %s",
            protocol,
            FileRetriever._getters[protocol].__name__,
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
                    delete=self.cleanup_at_exit and not self.__tmpdir,
                )

                # Select one auth config based inspection of url
                if self.auth_configs and not auth_config:
                    auth_config = _select_auth_scheme(url, self.auth_configs)

                # Validate auth_config if it exists
                if auth_config:
                    self._validate_auth_config(auth_config)
                    self.logger.info(
                        f'Selected `{auth_config["type"]}` authentication to '
                        f"fetch {url}"
                    )
                elif protocol != "file":
                    self.logger.warning(
                        f"Authentication scheme not found for url {url}"
                    )

                # Fetch the remote data
                FileRetriever._getters[protocol](
                    protocol,
                    path,
                    self._files[url],
                    logger=self.logger,
                    auth_config=auth_config,
                )
                if not hasattr(self._files[url], "original_name"):
                    filename = urlparse(url).path.rsplit("/", 1)[-1]
                    self._files[url].original_name = filename

            self._files[url].seek(0)
            return self._files[url]
        except Exception as e:
            if self.__tmpdir:
                self.__tmpdir.cleanup()
            raise e

    def _validate_auth_config(self, auth_config):
        """
        Validate config dict containing authentication parameters needed to
        access a particular URL.

        `auth_config` must be a dict with str key values, and have a
        key called `type`

        :param auth_configs: optional dict mapping URL patterns to
        authentication schemes and necessary auth parameters
        :type auth_configs: dict
        """
        assert_safe_type(auth_config, dict)
        assert_all_safe_type(list(auth_config.keys()), str)
        auth_scheme = auth_config.get("type")
        assert auth_scheme, (
            "An `auth_config` dict must have `type` "
            "key to define the authentication scheme"
        )
