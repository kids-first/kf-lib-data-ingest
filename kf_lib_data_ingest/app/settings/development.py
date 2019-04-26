"""
Testing settings for Kids First Ingest App
"""
import os

from kf_lib_data_ingest.app.settings.base import *

AUTH_CONFIGS.update({
    'https://kf-study-creator.kidsfirstdrc.org/download/study': {
        'type': 'token',
        'token_location': 'header',
        'token': os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN)
    }
})
