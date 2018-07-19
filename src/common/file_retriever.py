from abc import ABC

# TODO make a download class parse the protocol on the url for e.g. s3 vs http
# vs ftp vs file:// etc and handle appropriately.

# Download the file to the local application downloads folder, then return a
# handle to the file. There will be an application-level config parameter that
# determines whether to delete its downloads after the extraction stage is
# finished. This should use python's tempfile library for context managed
# deletion.


class FileRetriever(object):
    _clients = {}

    def __init__(self, storage_directory=None, cleanup_at_exit=True):
        pass

    def _interpret_protocol(self, url):
        pass

    def fetch(self, url):
        key = self._interpret_protocol(url)
        # TODO if key is file protocol, file copy, else download with client
        client = FileRetriever._clients.get(key)()
        if client is None:
            raise LookupError("No client found for protocol: " + key)
        pass # TODO use client to download file and then return it
        # return the file or contents or whatever




