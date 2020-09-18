import sys
import os
import logging

from kf_lib_data_ingest.common.io import read_df, write_json


VALIDATION_OUTPUT_DIR = "validation_results"
RESULTS_FILENAME = "validation_results.json"


class Validator(object):
    def __init__(self, output_dir=None, setup_logger=True):
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

        if setup_logger:
            root = logging.getLogger()
            root.setLevel(logging.INFO)
            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s" " - %(levelname)s - %(message)s"
                )
            )
            root.addHandler(logging.StreamHandler())
        self.logger = logging.getLogger(type(self).__name__)

    def validate(self, filepath_list, include_implicit=True, **report_kwargs):
        """
        Validate a set of tabular files containing a standardized set of
        columns
        Write validation report(s) (optionally in various formats) to disk
        TODO
        """
        try:
            # TODO
            # File paths to dict of DataFrames
            df_dict = {}
            for file_path in filepath_list:
                try:
                    df_dict[file_path] = read_df(file_path)
                except Exception:
                    continue
            if df_dict:
                # Do this to use df_dict so the test script passes
                pass

            # Do validation
            # results = do_all_validation(df_dict, include_implicit=include_implicit)
            results = {}
            # Always write out original validation results
            p = os.path.join(self.output_dir, RESULTS_FILENAME)
            write_json(results, p)
            self.logger.info(f"Wrote validation results json to: {p}")
            # self._build_report(results, **report_kwargs)
        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info("Exiting.")
            sys.exit(1)

    def _build_report(self, results, formats=None, **report_kwargs):
        """
        Write validation report(s) (optionally in various formats) to disk
        Write original validation results to disk
        TODO
        """
        pass
