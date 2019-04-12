import inspect
import logging
import os
import sys
from pprint import pformat

import pytest

import kf_lib_data_ingest.etl.stage_analyses as stage_analyses
from kf_lib_data_ingest.common.type_safety import assert_safe_type
from kf_lib_data_ingest.config import DEFAULT_TARGET_URL
from kf_lib_data_ingest.etl.configuration.dataset_ingest_config import (
    DatasetIngestConfig
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.auto import AutoTransformStage
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class DataIngestPipeline(object):

    def __init__(
        self, dataset_ingest_config_path, target_api_config_path,
        auth_configs=None, auto_transform=False, use_async=False,
        target_url=DEFAULT_TARGET_URL, log_level_name=None, log_dir=None,
        overwrite_log=None, dry_run=False
    ):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        :type  dataset_ingest_config_path: str
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

        assert_safe_type(dataset_ingest_config_path, str)
        assert_safe_type(target_api_config_path, str)
        assert_safe_type(auto_transform, bool)
        assert_safe_type(use_async, bool)
        assert_safe_type(target_url, str)
        assert_safe_type(log_level_name, None, str)
        assert_safe_type(log_dir, None, str)
        assert_safe_type(overwrite_log, None, bool)
        assert_safe_type(dry_run, bool)

        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path
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

        # Get log params from dataset_ingest_config
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

        # Log args, kwargs
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        kwargs = {arg: values[arg] for arg in args[2:]}
        # Don't log anything that might have secrets!
        kwargs.pop('auth_configs', None)
        self.logger.info(
            f'-- Ingest Params --\n{pformat(kwargs)}'
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
        if not self.auto_transform:
            transform_fp = self.data_ingest_config.transform_function_path
            if transform_fp:
                transform_fp = os.path.join(
                    self.ingest_config_dir, os.path.relpath(transform_fp)
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
            self.data_ingest_config.study['kf_id'],
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
        passed = True

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            # Iterate over stages and execute them
            output = None
            for stage in self._iterate_stages():
                self.stages[stage.stage_type] = stage
                if not output:  # First stage gets no input
                    output = stage.run()
                else:
                    output = stage.run(output)

                # Standard stage output validation
                if stage.concept_discovery_dict:
                    passed = self.check_stage_counts(stage) and passed

            # Run user defined data validation tests
            passed = self.user_defined_tests() and passed

        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info('Exiting.')
            sys.exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')
        return passed

    def check_stage_counts(self, stage):
        """
        Do some standard basic stage output tests like assessing whether there
        are as many unique values discovered for a given key as anticipated and
        also whether any values were lost between Extract and Transform.
        """
        stage.logger.info("Begin Basic Stage Output Validation")
        discovery_sources = stage.concept_discovery_dict.get('sources')

        # Missing data
        if not discovery_sources:
            stage.logger.info(
                f'Discovery Data Sources Not Found ❌')
            return False

        passed_all = True

        # Do stage counts validation
        passed, message = stage_analyses.check_counts(
            discovery_sources, self.data_ingest_config.expected_counts
        )
        passed_all = passed_all and passed
        stage.logger.info(message)

        # Compare stage counts to make sure we didn't lose values between
        # Extract and Transform
        if stage.stage_type == TransformStage:
            extract_disc = self.stages[ExtractStage].concept_discovery_dict
            if extract_disc and extract_disc.get('sources'):
                passed, message = stage_analyses.compare_counts(
                    discovery_sources,
                    extract_disc.get('sources')
                )
                passed_all = passed_all and passed
                stage.logger.info(message)
            else:
                # Missing data
                passed_all = False
                stage.logger.info(
                    'No ExtractStage Discovery Data Sources To Compare ❌'
                )
        stage.logger.info("End Basic Stage Output Validation")

        return passed_all

    def user_defined_tests(self):
        """
        Runs user defined data validation tests for the ingest package

        User defined tests must be placed in the `tests` directory inside of
        the ingest package dir (ingest_pipeline.ingest_config_dir).

        pytest is used to run these tests and so all tests should conform to
        the pytest standard.

        :returns boolean indiciating whether all tests passed or failed
        """
        passed = True
        user_defined_test_dir = os.path.join(self.ingest_config_dir,
                                             'tests')
        # No user defined tests exist
        if not os.path.isdir(user_defined_test_dir):
            self.logger.info('No user defined unit tests exist for ingest '
                             f'package: {self.ingest_config_dir}')
            return passed

        # Execute user defined unit tests
        self.logger.info('Running user defined data validation tests '
                         f'{user_defined_test_dir} ...')

        passed = pytest.main([user_defined_test_dir]) == 0

        if passed:
            self.logger.info(f'✅  User defined data validation tests passed!')
        else:
            self.logger.info(f'❌  User defined data validation tests passed!')

        return passed
