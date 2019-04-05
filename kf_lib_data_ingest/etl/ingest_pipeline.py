import inspect
import logging
import os
import sys
from pprint import pformat

import kf_lib_data_ingest.etl.stage_analyses as stage_analyses
from kf_lib_data_ingest.common.misc import write_json
from kf_lib_data_ingest.common.stage import IngestStage
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

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(
        self, dataset_ingest_config_path, target_api_config_path,
        auto_transform=False, use_async=False, target_url=DEFAULT_TARGET_URL,
        log_level_name=None, log_dir=None, overwrite_log=None
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

        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path
        )
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath
        )
        self.ingest_output_dir = os.path.join(self.ingest_config_dir, 'output')

        self.target_api_config_path = target_api_config_path
        self.auto_transform = auto_transform
        self.use_async = use_async
        self.target_url = target_url

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
        self.logger.info(
            f'-- Ingest Params --\n{pformat(kwargs)}'
        )

    def _iterate_stages(self):
        # Extract stage #######################################################

        yield ExtractStage(
            self.ingest_output_dir,
            self.data_ingest_config.extract_config_paths
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
            uid_cache_dir=self.ingest_output_dir, use_async=self.use_async
        )

    def run(self):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.
        """
        self.logger.info('BEGIN data ingestion.')
        self.stage_outputs = {}
        self.stage_discovery_sources = {}
        self.stage_discovery_links = {}
        passed = True

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            # Iterate over stages and execute them
            output = None
            for stage in self._iterate_stages():
                if not output:  # First stage gets no input
                    output, concept_discovery_dict = stage.run()
                else:
                    output, concept_discovery_dict = stage.run(output)

                # Collapse stage subtypes to their bases for storage
                # (e.g. GuidedTransformStage -> TransformStage)
                stage_type = type(stage)
                while stage_type.__base__ != IngestStage:
                    stage_type = stage_type.__base__

                self.stage_outputs[stage_type] = output

                if concept_discovery_dict:
                    write_json(
                        concept_discovery_dict,
                        os.path.join(
                            self.ingest_output_dir,
                            stage_type.__name__ + '_concept_discovery.json'
                        )
                    )

                    self.stage_discovery_sources[stage_type] = (
                        concept_discovery_dict.get('sources')
                    )
                    self.stage_discovery_links[stage_type] = (
                        concept_discovery_dict.get('links')
                    )

                    stage.logger.info("Begin Basic Stage Output Validation")
                    passed = (
                        passed
                        and self.check_stage_counts(stage_type, stage.logger)
                    )
                    stage.logger.info("End Basic Stage Output Validation")
        except Exception as e:
            self.logger.exception(str(e))
            self.logger.info('Exiting.')
            sys.exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')
        return not passed

    def check_stage_counts(self, stage_type, logger):
        """
        Do some standard basic stage output tests like assessing whether there
        are as many unique values discovered for a given key as anticipated and
        also whether any values were lost between Extract and Transform.
        """
        discovery_sources = self.stage_discovery_sources[stage_type]

        # Missing data
        if not discovery_sources:
            logger.info('Discovery Data Sources Not Found ❌')
            return False

        passed_all = True

        # Do stage counts validation
        passed, message = stage_analyses.check_counts(
            discovery_sources, self.data_ingest_config.expected_counts
        )
        passed_all = passed_all and passed
        logger.info(message)

        # Compare stage counts to make sure we didn't lose values between
        # Extract and Transform
        if stage_type == TransformStage:
            if ExtractStage in self.stage_discovery_sources:
                passed, message = stage_analyses.compare_counts(
                    discovery_sources,
                    self.stage_discovery_sources[ExtractStage]
                )
                passed_all = passed_all and passed
                logger.info(message)
            else:
                # Missing data
                passed_all = False
                logger.info(
                    'No Extract Discovery Data Sources To Compare ❌'
                )

        return passed_all
