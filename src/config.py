import os

DATASET_INGEST_CONFIG_DEFAULT_FILENAME = 'dataset_ingest_config.yml'
USE_ASYNC_KEY = 'use_async'
TARGET_URL_KEY = 'target_url'
DEFAULT_TARGET_URL = 'http://localhost:5000'
TARGET_SERVICE_CONFIG_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'target_services',
    'kids_first.py')
