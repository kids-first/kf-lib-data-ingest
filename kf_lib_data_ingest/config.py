import os
import logging

ROOT_DIR = os.path.dirname(__file__)

DATASET_INGEST_CONFIG_DEFAULT_FILENAME = 'dataset_ingest_config.yml'
DATA_INGEST_SCHEMA_PATH = os.path.join(ROOT_DIR,
                                       'etl', 'configuration',
                                       'data_ingest_config_schema.yml')
USE_ASYNC_KEY = 'use_async'
TARGET_URL_KEY = 'target_url'
DEFAULT_TARGET_URL = 'http://localhost:5000'
KIDSFIRST_DATASERVICE_PROD_URL = 'http://kf-api-dataservice.kidsfirstdrc.org'

DEFAULT_LOG_FILENAME = 'ingest.log'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_OVERWRITE_OPT = True

INGEST_PKG_TEMPLATE_NAME = 'my_study'

DEFAULT_ID_CACHE_FILENAME = 'uid_cache.json'
