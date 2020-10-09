"""
Abstract base class for validation report builders
"""
import os
import logging
from abc import ABC, abstractmethod
from pprint import pformat


SAMPLE_VALIDATION_RESULTS = os.path.abspath("./sample_validation_results.py")
PASSED = "success"
FAILED = "fail"
NA = "did not run"
RESULT_TO_EMOJI = {PASSED: "✅", FAILED: "❌", NA: "🔘"}


class AbstractReportBuilder(ABC):
    """
    Abstract base class defines the required interface for all
    subclass validation report builders

    Subclasses must implement methods:
    - `_build`: builds the report content in a particular format
    - `_write_report`: writes the report content to disk.

    The public `build` method will pass the validation results
    to the subclass's _build method

    See method docstrings for more details.
    """

    def __init__(self, output_dir=None, setup_logger=False):
        """
        Constructor

        :param output_dir: Directory where validation files will be written
        :type output_dir: str
        :param setup_logger: Whether to setup a console logger. Set to True if
        running standalone
        :type setup_logger: bool
        """
        self.output_dir = output_dir or os.getcwd()

        if setup_logger:
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

    @abstractmethod
    def _build(self, results, *args, **kwargs):
        """
        Build the validation report content from the validation `results` dict
        Subclasses must implement this method

        :param results: Validation result dicts.

        See kf_lib_data_ingest.validation.reporting.sample_validation_results.py # noqa E501
        for expected format

        :type results: list of dicts

        :returns: validation report content to be written to file
        """
        raise NotImplementedError()

    @abstractmethod
    def _write_report(self, content, *args, **kwargs):
        """
        Write the validation report content from _build to file(s)

        Subclasses must implement this method

        :param content: Validation report content
        :type content: Variable. Subclass is responsible to know how to handle
        the content type

        :returns: File path or directory where files have been written
        """
        raise NotImplementedError()

    def build(self, results, **report_kwargs):
        """
        Build validation report content from validation result dicts
        Write validation report files to disk

        :param results: See _build
        :type results: See _build
        :param report_kwargs: Keyword args passed to subclass _build
        (e.g. title='My Validation Report')
        :type report_kwargs: dict

        :returns: Validation report content to be written to file
        """
        self.logger.debug(
            "Checking validation results for schema conformance ..."
        )

        self.logger.info("Begin building validation report ...")
        output = self._build(results, **report_kwargs)

        # Write validation report files
        return self.write_report(output)

    def write_report(self, content):
        """
        Write validation report file(s) and log files written

        :param content: Validation report content
        :type content: Variable. Subclass is responsible to know how to handle
        the content type
        """
        path = self._write_report(content)
        if os.path.isdir(path):
            paths = [os.path.join(path, fp) for fp in os.listdir(path)]
        else:
            paths = [path]

        self.logger.info(f"Wrote validation report file(s):\n{pformat(paths)}")

        return paths

    def _result_code(self, result_dict):
        """
        Evaluate fields in validation result dict to determine test outcome
        code

        :param result_dict: validation result dict
        :type result_dict: dict
        """
        # Did not run
        if not result_dict["is_applicable"]:
            code = NA
        # Failed
        elif result_dict["is_applicable"] and result_dict["errors"]:
            code = FAILED
        # Passed
        else:
            code = PASSED

        return code
