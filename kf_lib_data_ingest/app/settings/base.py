"""
Base settings for Kids First Ingest App
"""

import os

from kf_lib_data_ingest.config import ROOT_DIR

TARGET_API_CONFIG = os.path.join(
    ROOT_DIR, "target_api_plugins", "kids_first_dataservice.py"
)


class SECRETS:
    KF_STUDY_CREATOR_API_TOKEN = "KF_STUDY_CREATOR_API_TOKEN"
    KF_SC_INGEST_APP_CLIENT_SECRET = "KF_SC_INGEST_APP_CLIENT_SECRET"
    WAREHOUSE_DB_URL = "KF_WAREHOUSE_DB_URL"


AUTH_CONFIGS = {
    # "http://localhost:8080/token/download": {
    #     "type": "token",
    #     "token_location": "url",
    #     "token": os.environ.get(SECRETS.KF_STUDY_CREATOR_API_TOKEN),
    # },
    # "https://localhost:8080/oath2/download": {
    #     "type": "oauth2",
    #     "provider_domain": os.environ.get("KF_AUTH0_DOMAIN"),
    #     "audience": os.environ.get("KF_STUDY_CREATOR_AUTH0_AUD"),
    #     "client_id": os.environ.get("KF_SC_INGEST_APP_CLIENT_ID"),
    #     "client_secret": os.environ.get(
    #         SECRETS.KF_SC_INGEST_APP_CLIENT_SECRET
    #     ),
    # },
}
