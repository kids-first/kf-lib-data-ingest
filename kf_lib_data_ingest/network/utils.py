"""
Common network (HTTP, TCP, whatever) related functionality
"""
import cgi
import logging
import os
import urllib.parse
from pprint import pformat

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from requests.packages.urllib3.util.retry import Retry

from kf_lib_data_ingest.common.io import read_json, write_json
from kf_lib_data_ingest.common.misc import upper_camel_case

logger = logging.getLogger(__name__)


def http_get_file(url, dest_obj, **kwargs):
    """
    Get the file at ``url`` and write to ``dest_obj``, a file-like obj

    :param url: the URL to send the GET request to
    :type url: ``str``
    :param dest_obj: a file-like object that receives the data downloaded
    :type dest_obj: a file-like object
    :param kwargs: keyword args forwarded to ``requests.get``
    :type kwargs: ``dict``
    :return: ``response``, a ``requests.Response`` object
    :rtype: ``requests.Response``
    """

    kwargs["stream"] = True
    response = RetrySession(connect=1).get(url, **kwargs)

    if response.status_code == 200:
        # Get filename from Content-Disposition header
        content_disposition = response.headers.get("Content-Disposition", "")
        _, cdisp_params = cgi.parse_header(content_disposition)
        filename = cdisp_params.get("filename*")
        # RFC 5987 ext-parameter is actually more complicated than this,
        # but this should get us 90% there, and the rfc6266 python lib is
        # broken after PEP 479. *sigh* - Avi K
        if filename and filename.lower().startswith("utf-8"):
            filename = urllib.parse.unquote(filename.split("'", 2)[2])
        else:
            filename = cdisp_params.get("filename")

        success_msg = f"Successfully fetched {url}"
        if filename:
            dest_obj.original_name = filename
            success_msg += f" with original file name {filename}"
        else:
            # Header did not provide filename
            logging.warning(
                f"{url} returned unhelpful or missing "
                "Content-Disposition header "
                f"{content_disposition}. "
                "HTTP(S) file responses should include a "
                "Content-Disposition header specifying "
                "filename."
            )

        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                dest_obj.write(chunk)

        dest_obj.seek(0)
        response.close()

        logger.info(success_msg)

    else:
        logger.error(f"Could not fetch {url}. Caused by: '{response.text}'")

    return response


class RetrySession(requests.Session):
    """
    Session for sending http requests with retry on failures or redirects

    See ``urllib3.Retry`` docs for details on all kwargs
    Modified source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests # noqa E501

    :param total: total retry attempts
    :param read: total retries on read errors
    :param connect: total retries on connection errors
    :param status: total retries on bad status codes defined in \
    ``status_forcelist``
    :param backoff_factor: affects sleep time between retries
    :param status_forcelist: ``list`` of HTTP status codes that force retry
    """

    def __init__(
        self,
        total=10,
        read=10,
        connect=10,
        status=10,
        backoff_factor=5,
        status_forcelist=(500, 502, 503, 504),
    ):
        self.logger = logging.getLogger(type(self).__name__)
        total = int(os.environ.get("MAX_RETRIES_ON_CONN_ERROR", total))

        super().__init__()

        retry = Retry(
            total=total,
            read=read,
            connect=connect,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            method_whitelist=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def send(self, req, **kwargs):
        self.logger.debug("Sending request: " + pformat(vars(req)))
        return super().send(req, **kwargs)


def get_open_api_v2_schema(
    url, entity_names, cached_schema_filepath=None, logger=None
):
    """
    Get schemas for entities in the target API using ``{url}/swagger``
    endpoint. Will extract parts of the ``{url}/swagger`` response to create the
    output ``dict``

    It is expected that swagger endpoint implements the OpenAPI v2.0
    spec. This method currently supports parsing of responses
    with a JSON mime-type.

    See link below for more details on the spec:

    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md

    Example response from ``/swagger`` endpoint::

        {
            'info': {
                'version': '1.9.0',
                'description':  'stuff',
                ...
            },
            'definitions': {
                'Participant': {
                    ...
                },
                'Biospecimen': {
                    ...
                },
                'BiospecimenResponse': {
                    ...
                }
                ...
            }
        }

        Will turn into the output:

        {
            'target_service': https://kf-api-dataservice.kidsfirstdrc.org,
            'version': 1.9.0,
            'definitions': {
                'participant': {
                    ...
                },
                'biospecimen': {
                    ...
                },
                ...
            }
        }

    Items in ``entity_names`` must be snake cased versions of existing keys in
    swagger ``definitions``.

    See https://github.com/OAI/OpenAPI-Specification/blob/master/examples/v2.0/json/petstore.json # noqa E501

    :param url: URL to a target service
    :param entity_names: ``list`` of snake cased names of entities to extract from\
    swagger ``definitions`` ``dict``
    :param cached_schema_filepath: file path to a ``JSON`` file containing a \
    saved version of the target service's schema.
    :param logger: logger to use when reporting errors
    :return: output, a ``dict`` with the schema definition and version
    :rtype: ``dict``
    """
    output = None
    err = None
    common_msg = "Unable to retrieve target schema from target service!"
    schema_url = f"{url}/swagger"

    # Create default cached_schema filepath
    if not cached_schema_filepath:
        cached_schema_filepath = os.path.join(os.getcwd(), "cached_schema.json")

    # Try to get schemas and version from the target service
    try:
        # ***** TODO remove connect=0, its a temporary hack!!! ***** #
        # Before connect=0, any non-mocked calls to unreachable APIs
        # like dataservice were causing tests to hang. What we really need
        # to do is remove this flag and do integration tests with a
        # live dataservice server - Natasha
        response = RetrySession(connect=0).get(schema_url)

    except ConnectionError as e:

        err = f"{common_msg}\nCaused by {str(e)}"

    else:
        if response.status_code == 200:
            # API Version
            version = response.json()["info"]["version"]
            # Schemas
            defs = response.json()["definitions"]
            schemas = {
                k: defs[upper_camel_case(k)]
                for k in entity_names
                if upper_camel_case(k) in defs
            }
            # Make output
            output = {
                "target_service": url,
                "definitions": schemas,
                "version": version,
            }
            # Update cache file
            write_json(output, cached_schema_filepath)
        else:
            err = f"{common_msg}\nCaused by unexpected response(s):"
            if response.status_code != 200:
                err += f"\nFrom {schema_url}/swagger:\n{response.text}"

    if err:
        if os.path.isfile(cached_schema_filepath):
            return read_json(cached_schema_filepath)
        else:
            err += (
                "\nTried loading from cache "
                f"but could not find file: {cached_schema_filepath}"
            )
        # Report error
        if logger:
            logger.warning(err)
        else:
            print(err)

    return output
