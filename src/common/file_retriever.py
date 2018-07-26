from abc import ABC

import boto3
import botocore
import requests
import tempfile
import shutil


def s3_save(s3_url, dest_obj, auth="saml"):
    """
    Get a file from Amazon S3.
    """
    bucket, key = s3_url.split("/", 1)
    s3 = boto3.session.Session(profile_name=auth).resource("s3")
    s3.Object(bucket, key).download_fileobj(dest_obj)


def web_save(url, dest_obj, auth=None):
    """
    Get a file from a web server.
    """
    with requests.get(url, auth=auth, stream=True) as response:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                dest_obj.write(chunk)


def file_save(path, dest_obj, auth=None):
    """
    Get a file from a local file path.
    """
    protocol = "file://"
    if path.startswith(protocol):
        path = path[len(protocol):]

    with open(path, "rb") as orig:
        shutil.copyfileobj(orig, dest_obj)


class FileRetriever(object):
    _getters = {
        "s3": s3_save,
        "http": web_save,
        "https": web_save,
        "file": file_save
    }

    def __init__(self, storage_dir=None, cleanup_at_exit=True):
        self.cleanup_at_exit = cleanup_at_exit
        if storage_dir:
            self.__tmpdir = None
            self.storage_dir = storage_dir
        else:
            self.__tmpdir = tempfile.TemporaryDirectory(prefix=".ingest_tmp_",
                                                        dir=".")
            self.storage_dir = self.__tmpdir.name
        self._files = {}

    def _find_protocol(self, url):
        split_url = url.split("://", 1)
        if len(split_url) == 2:
            return split_url[0].lower()
        else:
            return "file"

    def get(self, url, auth=None):
        protocol = self._find_protocol(url)
        if protocol not in FileRetriever._getters:
            raise LookupError(
                f"Retrieving URL: {url}\n"
                f"No client found for protocol: {protocol}"
            )

        print("Getting", url)

        try:  # TODO: eventually remove this try wrapper
            if url not in self._files:
                self._files[url] = tempfile.NamedTemporaryFile(
                    dir=self.storage_dir,
                    delete=self.cleanup_at_exit and not self.__tmpdir
                )
                FileRetriever._getters[protocol](url, self._files[url], auth)

            self._files[url].seek(0)
            return self._files[url]
        except Exception as e:
            if self.__tmpdir:
                self.__tmpdir.cleanup()
            raise e
