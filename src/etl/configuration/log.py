import logging
import os

from config import (
    DEFAULT_LOG_OVERWRITE_OPT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_FILENAME
)
from common.misc import timestamp


def setup_logger(log_dir, overwrite_log=DEFAULT_LOG_OVERWRITE_OPT,
                 log_level=DEFAULT_LOG_LEVEL):
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
    # Defaults
    filemode = 'w'
    filename = DEFAULT_LOG_FILENAME

    # Create a new log file named with a timestamp
    if not overwrite_log:
        filename = 'ingest-{}.log'.format(timestamp())

    log_filepath = os.path.join(log_dir, filename)

    # Setup log message formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setup rotating file handler
    fileHandler = logging.handlers.RotatingFileHandler(log_filepath,
                                                       mode=filemode)
    fileHandler.setFormatter(formatter)

    # Setup console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)

    # Set log level and handlers
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(fileHandler)
    root.addHandler(consoleHandler)

    return log_filepath
