import os
import logging

ROOT_DIR = os.path.dirname(__file__)
TEMPLATES_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'templates')

INGEST_PACKAGE_CONFIG_DEFAULT_FILENAME = 'ingest_package_config.py'

USE_ASYNC_KEY = 'use_async'
TARGET_URL_KEY = 'target_url'
DEFAULT_TARGET_URL = 'http://localhost:5000'

DEFAULT_LOG_FILENAME = 'ingest.log'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_OVERWRITE_OPT = True

INGEST_PKG_TEMPLATE_NAME = 'my_study'

DEFAULT_ID_CACHE_FILENAME = 'uid_cache.db'

print('hello')
