"""
Development settings for Kids First Ingest App
"""
import os
from kf_lib_data_ingest.app.settings.base import *


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
