import cgi
import logging
import os

import requests

from kf_lib_data_ingest.common.misc import timestamp_str

logger = logging.getLogger(__name__)


def get(url, dest_obj=None, **kwargs):
    """
    Use requests library to send a GET request to `url` with keyword args
    If kwargs contains the keyword arg `stream`=True, then download the
    response content to `dest_obj`, a file-like object.

    If `dest_obj` is not supplied, download to a file in the current working
    dir called `<ISO 8601 formatted timestamp>_downloaded_file`.
    See kf_lib_data_ingest.common.misc.timestamp_str for timestamp format.

    :param url: the URL to send the GET request to
    :type url: str
    :param dest_obj: a file-like object that receives the data downloaded
    :type dest_obj: a file-like object
    :returns response: requests.Response object
    """

    response = requests.get(url, **kwargs)

    if response.status_code == 200:
        # Regular resource
        if not kwargs.get('stream'):
            logger.info(f'Success! Fetched {url}')
            return response

        # File type resource
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

        dest_obj = dest_obj or open(
            os.path.join(os.getcwd(), f'{timestamp_str()}_downloaded_file'))

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

        logger.info(f'Resource is a file, downloading it to {filename}...')
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                dest_obj.write(chunk)
        response.close()

    else:
        logger.warning(f'Could not fetch {url}. Caused by '
                       f'{response.text}')

    return response
