"""
FileRetriever is a class for downloading the contents of remote files using
whatever mechanism is appropriate for the given protocol.
"""

import logging
import os
import shutil
import tempfile
from urllib.parse import urlparse

import boto3
import botocore
from requests.auth import HTTPBasicAuth

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
    If `auth` is provided, use that to get the resource - this is
    essentially basic auth

    If `auth_config` is provided, then determine authentication scheme by
    `url` inspection. Look for `url` in the keys of the `auth_config` dict.
    If found, then select auth config for this url, and load in the correct
    auth parameters required by the auth scheme from the environment.

    If neither `auth` nor `auth_config` are provided then assume the resource
    requires no auth. Just send a get request to fetch it

    Currently supports the following authentication schemes:
        - Basic Authentication
        - OAuth 2 Authentication

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
        # Select auth scheme and it's config
        selected_cfg = None
        if auth_config:
            for key, cfg in auth_config.items():
                if url.startswith(key):
                    selected_cfg = cfg
                    break

        # If no auth config for URL, fallback to fetch an unprotected file
        if not selected_cfg:
            logger.info(f'Authentication scheme not found for url {url}')
            utils.get_file(url, dest_obj)
            return

        logger.info(
            f'Selected `{cfg["type"]}` authentication scheme to fetch {url}')
        var_names = cfg['variable_names']

        # Basic auth
        if selected_cfg['type'] == 'basic':
            auth = HTTPBasicAuth(os.environ.get(var_names.get('username')),
                                 os.environ.get(var_names.get('password')))
            utils.get_file(url, dest_obj, auth=auth)

        # OAuth 2
        elif selected_cfg['type'] == 'oauth2':
            kwargs = {
                'provider_domain': os.environ.get(
                    var_names.get('provider_domain')),
                'audience': os.environ.get(var_names.get('audience')),
                'client_id': os.environ.get(var_names.get('client_id')),
                'client_secret': os.environ.get(var_names.get('client_secret'))
            }
            oauth2.get_file(url, dest_obj, **kwargs)

    else:
        # Fetch protected file using auth from kwargs
        if auth:
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

    def __init__(self, storage_dir=None, cleanup_at_exit=True):
        """
        :param storage_dir: optional specific tempfile storage location
        :type storage_dir: str
        :param cleanup_at_exit: should the temp files vanish at exit
        :type cleanup_at_exit: bool
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

    def get(self, url, auth=None, auth_config=None):
        """
        Retrieve the contents of a remote file.

        :param url: full file URL
        :type url: str
        :param auth: auth argument appropriate to type
        :type auth: various
        :param auth_config: optional dict mapping URL patterns to
         authentication schemes and environment variables containing necessary
         auth parameters
        :type auth_config: dict

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
                # Fetch the remote data
                FileRetriever._getters[protocol](
                    protocol, path, self._files[url],
                    auth=auth, auth_config=auth_config, logger=self.logger
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
