import shutil
import tempfile
from abc import ABC

import boto3
import botocore
import requests


_PROTOCOL_SEP = "://"


def _s3_save(protocol, source_loc, dest_obj, auth="saml"):
    """
    Get contents of a file from Amazon S3 to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth: optional S3 auth profile (defaults to "saml")
    :type auth: str

    :returns: None, the data goes to dest_obj
    """
    bucket, key = source_loc.split("/", 1)
    s3 = boto3.session.Session(profile_name=auth).resource("s3")
    s3.Object(bucket, key).download_fileobj(dest_obj)


def _web_save(protocol, source_loc, dest_obj, auth=None):
    """
    Get contents of a file from a web server to a local file-like object.

    :param protocol: URL protocol identifier
    :type protocol: str
    :param source_loc: address or path
    :type source_loc: str
    :param dest_obj: Receives the data downloaded from the source file
    :type dest_obj: File-like object
    :param auth: optional requests-compatible auth object
    :type auth: http://docs.python-requests.org/en/master/user/authentication/

    :returns: None, the data goes to dest_obj
    """
    url = protocol + _PROTOCOL_SEP + source_loc
    with requests.get(url, auth=auth, stream=True) as response:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                dest_obj.write(chunk)


def _file_save(protocol, source_loc, dest_obj, auth=None):
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

    :returns: None, the data goes to dest_obj
    """
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
        self.cleanup_at_exit = cleanup_at_exit
        if storage_dir:
            self.__tmpdir = None
            self.storage_dir = storage_dir
        else:
            self.__tmpdir = tempfile.TemporaryDirectory(prefix=".ingest_tmp_",
                                                        dir=".")
            self.storage_dir = self.__tmpdir.name
        self._files = {}

    def _split_protocol(self, url):
        """
        Split file url string into protocol and path.

        Expected format is always <protocol>://<path to file>, even for local
        files.

        :param url: URL of local or remote file
        :type url: string
        """
        split_url = url.split(_PROTOCOL_SEP, 1)
        if len(split_url) == 2:
            return split_url[0], split_url[1]
        else:
            return None, None

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
        protocol, path = self._split_protocol(url)
        if protocol not in FileRetriever._getters:
            raise LookupError(
                f"Retrieving URL: {url}"
                f"No client found for protocol: {protocol}"
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
                    protocol, path, self._files[url], auth
                )

            self._files[url].seek(0)
            return self._files[url]
        except Exception as e:
            if self.__tmpdir:
                self.__tmpdir.cleanup()
            raise e
