import os

from kf_lib_data_ingest.config import ROOT_DIR


TARGET_API_CONFIG = os.path.join(ROOT_DIR,
                                 'target_apis',
                                 'kids_first.py')

AUTH_CONFIGS = {
    'http://localhost:8080/download/study': {
        'type': 'token',
        'token_location': 'url',
        'token': os.environ.get('KF_STUDY_CREATOR_API_TOKEN'),
    },
    'http://kf-study-creator-dev.kids-first.io/download/study': {
        'type': 'token',
        'token_location': 'url',
        'token': os.environ.get('KF_STUDY_CREATOR_API_TOKEN')
    }
}
