import inspect
import logging
import os
from collections import OrderedDict
from pprint import pformat

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.config import DEFAULT_TARGET_URL
from kf_lib_data_ingest.etl.configuration.dataset_ingest_config import (
    DatasetIngestConfig
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.auto import AutoTransformStage
from kf_lib_data_ingest.etl.transform.guided import GuidedTransformStage

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(
        self, dataset_ingest_config_path, target_api_config_path=None,
        auto_transform=False, use_async=False, target_url=DEFAULT_TARGET_URL,
        log_level_name=None, log_dir=None, overwrite_log=None
    ):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        :param target_api_config_path: Path to the target api config file
        :param auto_transform: Boolean specifies whether to use automatic
        graph-based transformation or user guided transformation
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        :param log_level_name: case insensitive name of log level
        (i.e. info, debug, etc) to control logging output.
        See keys in logging._nameToLevel dict for all possible options
        :param log_dir: override for setting the directory to put logs in
        :param overwrite_log: override for whether to persist the previous log
        """
        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath)
        self.ingest_output_dir = os.path.join(self.ingest_config_dir, 'output')

        self.target_api_config_path = target_api_config_path
        self.auto_transform = auto_transform  # False
        self.use_async = use_async  # False
        self.target_url = target_url  # DEFAULT_TARGET_URL

        # Get args, kwargs
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        kwargs = {arg: values[arg] for arg in args[2:]}

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
        self.logger.info(
            '-- Ingest Params --\n\t{}'.format(pformat(kwargs))
        )

    def _iterate_stages(self):
        # Extract stage #######################################################

        yield ExtractStage(
            self.ingest_output_dir,
            self.data_ingest_config.extract_config_paths,
            self.data_ingest_config.expected_counts
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
        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            # Iterate over stages and execute them
            output = None
            for s in self._iterate_stages():
                if not output:  # First stage gets no input
                    output, checks_passed, accounting_data = s.run()
                else:
                    output, checks_passed, accounting_data = s.run(output)
        except Exception as e:
            msg = str(e) + '\nExiting.'
            self.logger.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')
