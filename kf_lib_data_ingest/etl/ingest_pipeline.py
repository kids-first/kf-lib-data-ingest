import contextlib
import inspect
import logging
import os
import sys
from pprint import pformat

import pytest

import kf_lib_data_ingest.etl.stage_analyses as stage_analyses
from kf_lib_data_ingest.common.concept_schema import UNIQUE_ID_ATTR
from kf_lib_data_ingest.common.misc import timestamp
from kf_lib_data_ingest.common.type_safety import assert_safe_type
from kf_lib_data_ingest.config import (
    DEFAULT_TARGET_URL,
    VERSION
)
from kf_lib_data_ingest.etl.configuration.ingest_package_config import (
    IngestPackageConfig
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.auto import AutoTransformStage
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage

CODE_TO_STAGE_MAP = {
    'e': ExtractStage,
    't': TransformStage,
    'l': LoadStage
}
# Char sequence representing the full ingest pipeline: e.g. 'etl'
DEFAULT_STAGES_TO_RUN_STR = ''.join(list(CODE_TO_STAGE_MAP.keys()))


class DataIngestPipeline(object):

    def __init__(
        self, ingest_package_config_path, target_api_config_path,
        auth_configs=None, auto_transform=False, use_async=False,
        target_url=DEFAULT_TARGET_URL, log_level_name=None, log_dir=None,
        overwrite_log=None, dry_run=False,
        stages_to_run_str=DEFAULT_STAGES_TO_RUN_STR
    ):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param ingest_package_config_path: Path to config file containing all
        parameters for data ingest.
        :type ingest_package_config_path: str
        :param target_api_config_path: Path to the target api config file
        :type target_api_config_path: str
        :param auto_transform: Whether to use automatic graph-based
        transformation, defaults to False (user guided transformation)
        :type auto_transform: bool, optional
        :param use_async: Whether to load asynchronously, defaults to False
        :type use_async: bool, optional
        :param target_url: The target API URL, defaults to DEFAULT_TARGET_URL
        :type target_url: str, optional
        :param log_level_name: Override the logging level (e.g. 'debug'),
        defaults to None (don't override)
        :type log_level_name: str, optional
        :param log_dir: Override the logfile directory,
        defaults to None (don't override)
        :type log_dir: str, optional
        :param overwrite_log: Override whether to persist the previous log,
        defaults to None (don't override)
        :type overwrite_log: bool, optional
        """

        assert_safe_type(ingest_package_config_path, str)
        assert_safe_type(target_api_config_path, str)
        assert_safe_type(auto_transform, bool)
        assert_safe_type(use_async, bool)
        assert_safe_type(target_url, str)
        assert_safe_type(log_level_name, None, str)
        assert_safe_type(log_dir, None, str)
        assert_safe_type(overwrite_log, None, bool)
        assert_safe_type(dry_run, bool)
        self._validate_stages_to_run_str(stages_to_run_str)

        self.data_ingest_config = IngestPackageConfig(
            ingest_package_config_path
        )
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath
        )
        self.ingest_output_dir = os.path.join(self.ingest_config_dir, 'output')

        self.target_api_config_path = target_api_config_path
        self.auth_configs = auth_configs
        self.auto_transform = auto_transform
        self.use_async = use_async
        self.target_url = target_url
        self.dry_run = dry_run
        self.stages_to_run_str = stages_to_run_str

        # Get log params from ingest_package_config
        log_dir = log_dir or self.data_ingest_config.log_dir
        log_kwargs = {param: getattr(self.data_ingest_config, param)
                      for param in ['overwrite_log', 'log_level']}

        if overwrite_log is not None:
            log_kwargs['overwrite_log'] = overwrite_log

        # Apply any log parameter overrides
        log_level = logging._nameToLevel.get(str(log_level_name).upper())
        if log_level:
            log_kwargs['log_level'] = log_level

        # Setup logger
        self.log_file_path = setup_logger(log_dir, **log_kwargs)
        self.logger = logging.getLogger(type(self).__name__)

        head, tail = os.path.split(self.log_file_path)
        self.counts_file_path = os.path.join(head, 'counts_for_' + tail)

        # Remove the previous counts file. This isn't important. I just don't
        # want the previous run's file sitting around until the new one gets
        # written. - Avi
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.counts_file_path)

        # Log args, kwargs
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        kwargs = {arg: values[arg] for arg in args[2:]}
        # Don't log anything that might have secrets!
        kwargs.pop('auth_configs', None)

        # Log ingest package dir, ingest lib version, and commit hash
        kwargs['ingest_package_dir'] = self.ingest_config_dir
        kwargs['version'] = VERSION

        self.logger.info(
            f'-- Ingest Params --\n{pformat(kwargs)}'
        )

    def _validate_stages_to_run_str(self, stages_to_run_str):
        """
        Validate `stages_to_run_str`, a char sequence where each char
        represents an ingest stage to run during pipeline execution

        Each char must be in the set of valid chars defined by the keys of
        CODE_TO_STAGE_MAP.

        Order of chars does not matter because the pipeline always executes
        stages (_iterate_stages) in the correct order.
        """
        assert_safe_type(stages_to_run_str, str)

        valid_char_set = set(CODE_TO_STAGE_MAP.keys())
        assert all([c in valid_char_set
                    for c in stages_to_run_str]), (
            f'Invalid value for stages to run option: "{stages_to_run_str}"! '
            f'Each char must exist in the valid set: {valid_char_set}'
        )

    def _iterate_stages(self):
        # Extract stage #######################################################

        yield ExtractStage(
            self.ingest_output_dir,
            self.data_ingest_config.extract_config_paths,
            self.auth_configs
        )

        # Transform stage #####################################################

        transform_fp = None
        # Create file path to transform function Python module
        if (
            self.data_ingest_config.transform_function_path
            and not self.auto_transform
        ):
            transform_fp = os.path.join(
                self.ingest_config_dir,
                os.path.relpath(
                    self.data_ingest_config.transform_function_path
                )
            )

        if not transform_fp:
            # ** Temporary - until auto transform is further developed **
            raise FileNotFoundError(
                'Transform module file has not been created yet! '
                'You must define a transform function in order for ingest '
                'to continue.')
            #
            # yield AutoTransformStage(
            #     self.target_api_config_path, self.target_url,
            #     self.ingest_output_dir
            # )
        else:
            yield GuidedTransformStage(
                transform_fp, self.target_api_config_path, self.target_url,
                self.ingest_output_dir
            )

        # Load stage ##########################################################

        yield LoadStage(
            self.target_api_config_path, self.target_url,
            self.data_ingest_config.target_service_entities,
            self.data_ingest_config.study,
            uid_cache_dir=self.ingest_output_dir, use_async=self.use_async,
            dry_run=self.dry_run
        )

    def run(self):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.
        """
        self.logger.info('BEGIN data ingestion.')
        self.stages = {}
        stages_to_run = {CODE_TO_STAGE_MAP[c] for c in self.stages_to_run_str}
        all_passed = True
        all_messages = []

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            # Iterate over stages and execute them
            output = None
            for stage in self._iterate_stages():
                # Only run stages that were specified in stages_to_run
                if stage.stage_type not in stages_to_run:
                    previous_stage = stage
                    continue

                self.stages[stage.stage_type] = stage

                if isinstance(stage, ExtractStage):
                    output = stage.run()  # First stage gets no input
                else:
                    # Try using cached output of previous stage
                    if not output:
                        self.logger.info(
                            'Using cached input for '
                            f'{type(previous_stage).__name__}'
                        )
                        output = previous_stage.read_output()
                    output = stage.run(output)

                # Standard stage output validation
                if stage.concept_discovery_dict:
                    passed, messages = self.check_stage_counts(stage)
                    all_passed = passed and all_passed
                    all_messages.extend(messages)

            self._log_count_results(all_passed, all_messages)

            # Run user defined data validation tests
            all_passed = self.user_defined_tests() and all_passed

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info('Exiting.')
            sys.exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')
        return all_passed

    def _log_count_results(self, all_passed, messages):
        if all_passed:
            summary = '✅ Count Analysis Passed!\n'
        else:
            summary = '❌ Count Analysis Failed!\n'

        self.logger.info(summary + f'See {self.counts_file_path} for details')

        summary += (
            'Ingest package: '
            f'{self.data_ingest_config.config_filepath}\n'
            f'Completed at: {timestamp()}'
        )
        header = [
            '======================\n'
            'COUNT ANALYSIS RESULTS\n'
            '======================',
            summary
        ]
        doc = '\n\n'.join(header + messages)
        with open(self.counts_file_path, 'w') as cfp:
            cfp.write(doc)
        self.logger.debug(doc)

    def check_stage_counts(self, stage):
        """
        Do some standard basic stage output tests like assessing whether there
        are as many unique values discovered for a given key as anticipated and
        also whether any values were lost between Extract and Transform.
        """
        stage_name = stage.stage_type.__name__
        stage.logger.info('Begin Basic Stage Output Validation')
        discovery_sources = stage.concept_discovery_dict.get('sources')

        all_messages = [stage_name + '\n' + '='*len(stage_name)]

        # Missing data
        if not discovery_sources:
            stage.logger.info('❌ Discovery Data Sources Not Found')
            return False, all_messages

        passed_all = True

        # Do stage counts validation
        passed, messages = stage_analyses.check_counts(
            discovery_sources, self.data_ingest_config.expected_counts
        )
        all_messages.extend(messages)

        passed_all = passed_all and passed

        # Compare stage counts to make sure we didn't lose values between
        # Extract and Transform
        if stage.stage_type == TransformStage and ExtractStage in self.stages:
            extract_disc = self.stages[ExtractStage].concept_discovery_dict
            if extract_disc and extract_disc.get('sources'):
                passed, messages = stage_analyses.compare_counts(
                    ExtractStage.__name__,
                    extract_disc['sources'],
                    TransformStage.__name__,
                    {
                        k: v for k, v in discovery_sources.items()
                        if UNIQUE_ID_ATTR not in k
                    }
                )
                passed_all = passed_all and passed
                all_messages.extend(messages)
            else:
                # Missing data
                passed_all = False
                stage.logger.info(
                    '❌ No ExtractStage Discovery Data Sources To Compare'
                )
        stage.logger.info('End Basic Stage Output Validation')

        return passed_all, all_messages

    def user_defined_tests(self):
        """
        Runs user defined data validation tests for the ingest package

        User defined tests must be placed in the `tests` directory inside of
        the ingest package dir (ingest_pipeline.ingest_config_dir).

        pytest is used to run these tests and so all tests should conform to
        the pytest standard.

        :returns boolean indiciating whether all tests passed or failed
        """
        user_defined_test_dir = os.path.join(self.ingest_config_dir,
                                             'tests')
        # No user defined tests exist
        if not os.path.isdir(user_defined_test_dir):
            self.logger.info('No user defined unit tests exist for ingest '
                             f'package: {self.ingest_config_dir}')
            return True

        # Execute user defined unit tests
        self.logger.info('Running user defined data validation tests '
                         f'{user_defined_test_dir} ...')

        exit_code = pytest.main([user_defined_test_dir])

        # Check if exit code is one of the codes that should result in pass
        # Exit code 0:	All tests were collected and passed successfully
        # Exit code 5:	No tests were collected
        passed = exit_code in {0, 5}

        if exit_code == 0:
            self.logger.info(f'✅ User defined data validation tests passed')
        elif exit_code == 5:
            self.logger.warning(
                f'⚠️ pytest did not collect any user defined tests, '
                'even though user tests directory exists: '
                f'{user_defined_test_dir}')
        else:
            self.logger.info(f'❌ User defined data validation tests failed '
                             f'with exit_code {exit_code}')

        return passed
