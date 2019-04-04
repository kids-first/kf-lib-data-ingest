"""
Common network (HTTP, TCP, whatever) related functionality
"""
import cgi
import logging

from kf_lib_data_ingest.common.misc import requests_retry_session

logger = logging.getLogger(__name__)


def http_get_file(url, dest_obj, **kwargs):
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
    response = requests_retry_session(connect=1).get(url, **kwargs)

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
