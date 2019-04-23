import os

from kf_lib_data_ingest.config import ROOT_DIR


TARGET_API_CONFIG = os.path.join(ROOT_DIR,
                                 'target_apis',
                                 'kids_first.py')


class ENV_KEYS:
    KF_STUDY_CREATOR_API_TOKEN = 'KF_STUDY_CREATOR_API_TOKEN'
    KF_AUTH0_DOMAIN = 'KF_AUTH0_DOMAIN'
    KF_STUDY_CREATOR_AUTH0_AUD = 'KF_STUDY_CREATOR_AUTH0_AUD'
    KF_SC_INGEST_APP_CLIENT_ID = 'KF_SC_INGEST_APP_CLIENT_ID'
    KF_SC_INGEST_APP_CLIENT_SECRET = 'KF_SC_INGEST_APP_CLIENT_SECRET'


AUTH_CONFIGS = {
    'http://localhost:8080/download/study': {
        'type': 'token',
        'token_location': 'url',
        'token': os.environ.get(ENV_KEYS.KF_STUDY_CREATOR_API_TOKEN),
    },
    'http://kf-study-creator-dev.kids-first.io/download/study': {
        'type': 'token',
        'token_location': 'url',
        'token': os.environ.get(ENV_KEYS.KF_STUDY_CREATOR_API_TOKEN)
    }
}
