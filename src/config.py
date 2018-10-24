import logging
import logging.handlers
import os

ROOT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)))

DATASET_INGEST_CONFIG_DEFAULT_FILENAME = 'dataset_ingest_config.yml'
DATA_INGEST_SCHEMA_PATH = os.path.join(ROOT_DIR,
                                       'etl', 'configuration',
                                       'data_ingest_config_schema.yml')
USE_ASYNC_KEY = 'use_async'
TARGET_URL_KEY = 'target_url'
DEFAULT_TARGET_URL = 'http://localhost:5000'
TARGET_SERVICE_CONFIG_PATH = os.path.join(ROOT_DIR,
                                          'target_services',
                                          'kids_first.py')
DEFAULT_LOG_FILENAME = 'ingest.log'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_OVERWRITE_OPT = True

INGEST_OP = 'ingest'
EXTRACT_OP = 'extract'
TRANSFORM_OP = 'transform'
LOAD_OP = 'load'
