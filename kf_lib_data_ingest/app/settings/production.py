"""
Production settings for Kids First Ingest App
"""

from kf_lib_data_ingest.app.settings.base import *

import os

AUTH_CONFIGS.update({
    'https://kf-study-creator.kidsfirstdrc.org/download/study': {
        'type': 'oauth2',
        'provider_domain': os.environ.get(ENV_KEYS.KF_AUTH0_DOMAIN),
        'audience': os.environ.get(ENV_KEYS.KF_STUDY_CREATOR_AUTH0_AUD),
        'client_id': os.environ.get(ENV_KEYS.KF_SC_INGEST_APP_CLIENT_ID),
        'client_secret': os.environ.get(
            ENV_KEYS.KF_SC_INGEST_APP_CLIENT_SECRET
        )
    }
})
