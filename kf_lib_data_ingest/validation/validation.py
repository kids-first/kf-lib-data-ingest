import os
import sys
import logging

from kf_lib_data_ingest.common.io import read_df, write_json
from kf_lib_data_ingest.validation.data_validator import (
    Validator as DataValidator
)

VALIDATION_OUTPUT_DIR = "validation_results"
RESULTS_FILENAME = "validation_results.json"


def check_results(results):
    """
    Validation passes if every test in `results` has no errors. Otherwise
    validation fails

    This method is called when `results` was read from file and you only need
    to evaluate whether validation passed or not

    :param results: Validation results. See sample_validation_results.py in
    kf_lib_data_ingest.validation.reporting for an example
    :type results: dict

    :returns: whether validation passed or not
    """

    return all([not r['errors'] for r in results['validation']])


class Validator(object):
    def __init__(self, output_dir=None, init_logger=True):
        """
        Constructor

        :param output_dir: Directory where validation files will be written
        :type output_dir: str
        :param setup_logger: Whether to setup a console logger. Set to True if
        running standalone
        :type setup_logger: bool
        """
        self.output_dir = output_dir or os.path.join(
            os.getcwd(), VALIDATION_OUTPUT_DIR
        )
        os.makedirs(self.output_dir, exist_ok=True)

        if init_logger:
            root = logging.getLogger()
            root.setLevel(logging.DEBUG)
            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s" " - %(levelname)s - %(message)s"
                )
            )
            root.addHandler(consoleHandler)
        self.logger = logging.getLogger(type(self).__name__)

    def validate(self, filepath_list, include_implicit=True, report_kwargs={}):
        """
        Validate a set of tabular files containing a standardized set of
        columns and write validation report(s) to disk
        """
        try:
            # File paths to dict of DataFrames
            df_dict = {}
            for file_path in filepath_list:
                try:
                    df_dict[file_path] = read_df(file_path)
                    self.logger.info(f'Loaded file: {file_path}')
                except Exception:
                    self.logger.info(f'Skipped file: {file_path}')
                    continue

            # Do validation
            results = DataValidator().validate(
                df_dict, include_implicit=include_implicit
            )
            # Always write out original validation results
            p = os.path.join(self.output_dir, RESULTS_FILENAME)
            write_json(results, p)
            self.logger.info(f"Wrote validation results json to: {p}")

            # Build and write validation reports to disk
            # TODO - Uncomment later
            # self._build_report(results, report_kwargs=report_kwargs)

            return check_results(results)

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info("Exiting.")
            sys.exit(1)

    def _build_report(self, results, formats=None, report_kwargs={}):
        # TODO - Fill in later
        pass
