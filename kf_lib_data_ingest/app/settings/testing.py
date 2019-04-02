"""
Testing settings for Kids First Ingest App
"""
import os

from kf_lib_data_ingest.app.settings.base import *

AUTH_CONFIGS = {
    'https://kf-study-creator.kidsfirstdrc.org/download/study': {
        'type': 'token',
        'token_location': 'url',
        'token': os.environ.get('KF_STUDY_CREATOR_API_TOKEN'),
    }
}
