"""
Common network (HTTP, TCP, whatever) related functionality
"""
import cgi
import logging
import os
import urllib.parse

from d3b_utils.requests_retry import Session

from kf_lib_data_ingest.common.io import read_json, write_json

module_logger = logging.getLogger(__name__)


def http_get_file(url, dest_obj, **kwargs):
    """
    Get the file at `url` and write to `dest_obj`, a file-like obj

    :param url: the URL to send the GET request to
    :type url: str
    :param dest_obj: a file-like object that receives the data downloaded
    :type dest_obj: a file-like object
    :param kwargs: keyword args forwarded to requests.get
    :type kwargs: dict
    :return: response, a requests.Response object
    :rtype: requests.Response
    """

    kwargs["stream"] = True
    logger = kwargs.get("logger", module_logger)

    response = Session().get(url, **kwargs)

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


def get_open_api_v2_schema(url, cached_schema_filepath=None, logger=None):
    """
    Get schemas for entities in the target API using {url}/swagger
    endpoint. Will extract parts of the {url}/swagger response to create the
    output dict

    It is expected that swagger endpoint implements the OpenAPI v2.0
    spec. This method currently supports parsing of responses
    with a JSON mime-type.

    See link below for more details on the spec:

    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md

    Example response from /swagger endpoint:
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
            'Participant': {
                ...
            },
            'Biospecimen': {
                ...
            },
            ...
        }
    }

    See https://github.com/OAI/OpenAPI-Specification/blob/master/examples/v2.0/json/petstore.json # noqa E501

    :param url: URL to a target service
    :param cached_schema_filepath: file path to a JSON file containing a
    saved version of the target service's schema.
    :param logger: logger to use when reporting errors
    :return: output, a dict with the schema definition and version
    :rtype: dict
    """
    logger = logger or module_logger
    output = None
    err = None
    common_msg = "Unable to retrieve target schema from target service!"
    schema_url = f"{url}/swagger"

    # Create default cached_schema filepath
    if not cached_schema_filepath:
        cached_schema_filepath = os.path.join(os.getcwd(), "cached_schema.json")

    # Try to get schemas and version from the target service
    try:
        response = Session().get(schema_url)
    except ConnectionError as e:
        err = f"{common_msg}\nCaused by {str(e)}"

    else:
        if response.status_code == 200:
            # API Version
            version = response.json()["info"]["version"]
            # Schemas
            defs = response.json()["definitions"]
            # Make output
            output = {
                "target_service": url,
                "definitions": defs,
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
        logger.warning(err)

    return output
