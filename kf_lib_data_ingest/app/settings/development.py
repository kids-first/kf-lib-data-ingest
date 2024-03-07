"""
Development settings for Kids First Ingest App
"""

import os

from kf_lib_data_ingest.app.settings.base import (
    SECRETS,
    TARGET_API_CONFIG,
)

TARGET_API_CONFIG = TARGET_API_CONFIG

KF_SC_TOKEN_CONFIG = {
    "type": "token",
    "token_location": "header",
    "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
}

AUTH_CONFIGS = {
    "http://localhost:8080/download/study": {
        "type": "token",
        "token_location": "header",
        "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
    },
    "https://kf-study-creator-dev.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
    "https://kf-api-study-creator-dev.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
    "https://kf-study-creator-qa.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
    "https://kf-api-study-creator-qa.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
    "https://kf-study-creator.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
    "https://kf-api-study-creator.kidsfirstdrc.org/download": KF_SC_TOKEN_CONFIG,
}
