"""
Production settings for Kids First Ingest App
"""
import os

from kf_lib_data_ingest.app.settings.base import (
    AUTH_CONFIGS,
    SECRETS,
    TARGET_API_CONFIG,
)

TARGET_API_CONFIG = TARGET_API_CONFIG

AUTH_CONFIGS.update(
    {
        "https://kf-study-creator.kidsfirstdrc.org/download/study": {
            "type": "oauth2",
            "provider_domain": os.environ.get("KF_AUTH0_DOMAIN"),
            "audience": os.environ.get("KF_STUDY_CREATOR_AUTH0_AUD"),
            "client_id": os.environ.get("KF_SC_INGEST_APP_CLIENT_ID"),
            "client_secret": os.environ.get(
                SECRETS.KF_SC_INGEST_APP_CLIENT_SECRET
            ),
        }
    }
)
