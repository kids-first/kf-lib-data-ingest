import logging
import logging.handlers
import os
import re

from kf_lib_data_ingest.app.settings.base import SECRETS
from kf_lib_data_ingest.common.misc import timestamp
from kf_lib_data_ingest.config import (
    DEFAULT_LOG_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT,
)

VERBOTEN_PATTERNS = {
    re.escape(os.environ[v]): f"<env['{v}']>"
    for k, v in SECRETS.__dict__.items()
    if not k.startswith("_") and os.environ.get(v)
}

VERBOTEN_PATTERNS['"access_token":".+"'] = '"access_token":"<ACCESS_TOKEN>"'
VERBOTEN_PATTERNS[
    "'Authorization': '.+'"
] = "'Authorization': '<AUTHORIZATION>'"


class NoTokenFormatter(logging.Formatter):
    def format(self, record):
        s = super().format(record)
        for k, v in VERBOTEN_PATTERNS.items():
            s = re.sub(k, v, s)
        return s


DEFAULT_FORMAT = (
    "%(asctime)s - %(name)s"
    " - Thread: %(threadName)s - %(levelname)s - %(message)s"
)
DEFAULT_FORMATTER = NoTokenFormatter(DEFAULT_FORMAT)


def setup_logger(
    log_dir,
    overwrite_log=DEFAULT_LOG_OVERWRITE_OPT,
    log_level=DEFAULT_LOG_LEVEL,
):
    """
    Configure and create the logger

    :param log_dir: the path to the log directory
    :param overwrite_log: a boolean specifying whether to create new log files
    or overwrite a defaul log file 'ingest.log'
    :param log_level: a string specifying what level of log messages to record
    in the log file. Values are not case sensitive. The list of acceptable
    values are the names of Python's standard lib logging levels.
    (critical, error, warning, info, debug, notset)
    """
    # Default file name
    filename = DEFAULT_LOG_FILENAME

    # Create a new log file named with a timestamp
    if not overwrite_log:
        filename = "ingest-{}.log".format(timestamp())

    log_filepath = os.path.join(log_dir, filename)

    # Setup rotating file handler
    fileHandler = logging.handlers.RotatingFileHandler(log_filepath, mode="w")
    fileHandler.setFormatter(DEFAULT_FORMATTER)

    # Setup console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(DEFAULT_FORMATTER)

    # Set log level and handlers
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(fileHandler)
    root.addHandler(consoleHandler)

    return log_filepath
