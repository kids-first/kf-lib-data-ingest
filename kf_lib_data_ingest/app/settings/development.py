"""
Development settings for Kids First Ingest App
"""
import os

from kf_lib_data_ingest.app.settings.base import (
    SECRETS,
    TARGET_API_CONFIG,
)

TARGET_API_CONFIG = TARGET_API_CONFIG

AUTH_CONFIGS = {
    "http://localhost:8080/download/study": {
        "type": "token",
        "token_location": "url",
        "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
    },
    "http://kf-study-creator-dev.kids-first.io/download/study": {
        "type": "token",
        "token_location": "header",
        "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
    },
    "https://kf-study-creator.kidsfirstdrc.org/download/study": {
        "type": "token",
        "token_location": "header",
        "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
    },
}
