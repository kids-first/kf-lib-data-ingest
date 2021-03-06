import logging
import os

try:  # Python 3.8
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

NETWORK_USER_AGENT = "kf-lib-data-ingest"

ROOT_DIR = os.path.dirname(__file__)
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")

INGEST_PACKAGE_CONFIG_DEFAULT_FILENAME = "ingest_package_config.py"

USE_ASYNC_KEY = "use_async"
TARGET_URL_KEY = "target_url"
DEFAULT_TARGET_URL = "http://localhost:5000"

DEFAULT_LOG_FILENAME = "ingest.log"
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_OVERWRITE_OPT = True

INGEST_PKG_TEMPLATE_NAME = "my_ingest_package"

DEFAULT_ID_CACHE_FILENAME = "uid_cache.db"

VERSION = version("kf-lib-data-ingest")


# Key in transform func's output dict whose value is the default DataFrame
# to use when transforming from DataFrame into target concept instances
DEFAULT_KEY = "default"
