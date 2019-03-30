import argparse
import json
import os
from pprint import pprint

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

from kf_lib_data_ingest.network import oauth2

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '8080'


def _make_request_data(study_kf_id, filepath):

    query = '''
            mutation ($file: Upload!, $studyId: String!) {
              createFile(file: $file, studyId: $studyId) {
                success
                file {
                    id
                    kfId
                    name
                    downloadUrl
                }
              }
            }
    '''

    m = MultipartEncoder(
        fields={
            'operations': json.dumps({
                'query': query.strip(),
                'variables': {
                    'file': None,
                    'studyId': study_kf_id
                }
            }),
            'map': json.dumps({
                '0': ['variables.file'],
            }),
            '0': (os.path.basename(filepath),
                  open(filepath, 'rb'), 'text/plain')}
    )

    return m


def upload_files(study_id, data_dir, url=None, host=None, port=None,
                 auth_params=None):
    if not url:
        host = host or DEFAULT_HOST
        port = port or DEFAULT_PORT
        url = f'http://{host}:{port}/graphql'

    data_dir = os.path.realpath(data_dir)
    headers = {}
    if auth_params:
        token = oauth2.get_service_token(auth_params.get('provider_domain'),
                                         auth_params.get('audience'),
                                         auth_params.get('client_id'),
                                         auth_params.get('client_secret'))
        if token:
            headers.update({'Authorization': f'Bearer {token}'})

    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if os.path.isdir(filepath):
            continue

        print(f'Uploading {filepath}')

        m = _make_request_data(study_id, filepath)
        headers.update({'Content-Type': m.content_type})
        response = requests.post(url, data=m, headers=headers)

        if response.status_code == 200:
            content = response.json()
            pprint(content.get('data'))
        else:
            print(response.text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("study_id", help='Kids First ID of existing study')
    parser.add_argument("data_dir", help='Directory of files to upload')
    parser.add_argument("--url", help="URL")
    parser.add_argument("--host", help="Host")
    parser.add_argument("--port", help="Port")
    args = parser.parse_args()

    auth_params = {
        'provider_domain': os.environ.get('KF_AUTH0_DOMAIN'),
        'audience': os.environ.get('SC_AUTH0_AUD'),
        'client_id': os.environ.get('SC_INGEST_APP_CLIENT_ID'),
        'client_secret': os.environ.get('SC_INGEST_APP_CLIENT_SECRET')
    }

    upload_files(args.study_id, args.data_dir, host=args.host, port=args.port,
                 auth_params=auth_params, url=args.url)
