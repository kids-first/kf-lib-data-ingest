import logging
import os
import sys

from kf_lib_data_ingest.common.io import read_df, write_json
from kf_lib_data_ingest.common.type_safety import assert_safe_type
from kf_lib_data_ingest.validation.data_validator import (
    Validator as DataValidator,
)
from kf_lib_data_ingest.validation.reporting.html import HtmlReportBuilder
from kf_lib_data_ingest.validation.reporting.markdown import (
    MarkdownReportBuilder,
)
from kf_lib_data_ingest.validation.reporting.table import TableReportBuilder

VALIDATION_OUTPUT_DIR = "validation_results"
RESULTS_FILENAME = "validation_results.json"
REPORT_BUILDERS = {
    "tsv": TableReportBuilder,
    "md": MarkdownReportBuilder,
    "html": HtmlReportBuilder,
}


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
    return all([not r["errors"] for r in results["validation"]])


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
        self.report_file_paths = []
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

        :param filepath_list: List of paths of files to validate
        :type filepath_list: list
        :param include_implicit: whether to account for implied connections
        :type include_implicit: bool
        :param report_kwargs: Keyword arguments for each report builder
        Forwarded to Validator._build_report
        :type report_kwargs: dict

        :returns: boolean indicating whether validation passed
        """
        try:
            # File paths to dict of DataFrames
            df_dict = {}
            for file_path in filepath_list:
                try:
                    df_dict[file_path] = read_df(file_path)
                    self.logger.info(f"Loaded file: {file_path}")
                except Exception:
                    self.logger.info(f"Skipped file: {file_path}")
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
            self.report_file_paths = self._build_report(
                results, report_kwargs=report_kwargs
            )

            return check_results(results)

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info("Exiting.")
            sys.exit(1)

    def _build_report(self, results, formats=None, report_kwargs={}):
        """
        Write validation report(s) (optionally in various formats) to disk
        Write original validation results to disk

        The `report_kwargs` is a dict of keyword argument dicts, where keys
        are the same as keys in REPORT_BUILDERS and values are the keyword
        arguments for a specific report builder. The builder's kwargs are
        passed to its `build` method.

        For example:

        report_kwargs = {
            'md': {'title': 'My Report'},
            'html': {'title': 'My Report', 'other_setting': 'foo'}
        }
        MarkdownReportBuilder().build(**report_kwargs['md'])

        :param results: validation results
        :type results: dict
        :param formats: iterable of format strings. See keys of REPORT_BUILDERS
        for current list of accepted formats
        :type formats: iterable
        :param report_kwargs: Keyword argument dicts passed to the report
        builder `build` methods.
        :type report_kwargs: dict

        :returns: list of report file paths
        """
        assert_safe_type(report_kwargs, dict)
        paths = []
        formats = formats or REPORT_BUILDERS.keys()
        for f in formats:
            if f in REPORT_BUILDERS:
                paths.extend(
                    REPORT_BUILDERS[f](output_dir=self.output_dir).build(
                        results, **report_kwargs.get(f, {})
                    )
                )
            else:
                self.logger.warning(
                    f"Validation report format `{f}` not supported"
                )
        return paths
