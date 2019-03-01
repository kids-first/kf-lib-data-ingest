import inspect
import logging
import os
from collections import OrderedDict
from pprint import pformat

from kf_lib_data_ingest.config import (
    DEFAULT_TARGET_URL,
    DEFAULT_LOG_LEVEL
)
from kf_lib_data_ingest.etl.configuration.dataset_ingest_config import (
    DatasetIngestConfig,
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        """
        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath)
        self.ingest_output_dir = os.path.join(self.ingest_config_dir,
                                              'output')

    def run(self, target_api_config_path, auto_transform=False,
            use_async=False, target_url=DEFAULT_TARGET_URL,
            log_level=None):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.

        See _run method for param description
        """
        # Get args, kwargs
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        kwargs = {arg: values[arg] for arg in args[2:]}

        # Create and setup logger
        self._create_logger(self.data_ingest_config, kwargs)

        # Log the start of the run with ingestion parameters
        run_msg = ('BEGIN data ingestion.\n\t-- Ingestion params --\n\t{}'
                   .format(pformat(kwargs)))
        self.logger.info(run_msg)

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            self._run(target_api_config_path, **kwargs)
        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def _create_logger(self, dataset_ingest_config, cli_options):
        """
        Create and setup logging for ingest

        Log parameters from the CLI options override log parameters in the
        dataset_ingest_config which override any library defaults defined
        in kf_lib_data_ingest.config.

        :param data_ingest_config: the DatasetIngestConfig for the ingest
        pipeline
        :param run_kwargs: CLI options which got passed as kwargs to the
        run method
        """
        # Get log params from dataset_ingest_config
        log_dir = self.data_ingest_config.log_dir
        opt_log_params = {param: getattr(self.data_ingest_config, param)
                          for param in ['overwrite_log', 'log_level']}

        # Apply CLI overrides
        log_level = cli_options.pop('log_level')
        if log_level:
            opt_log_params['log_level'] = log_level

        # Setup logger
        setup_logger(log_dir, **opt_log_params)
        self.logger = logging.getLogger(__name__)

    def _run(self, target_api_config_path, auto_transform=False,
             use_async=False, target_url=DEFAULT_TARGET_URL):
        """
        Runs the ingest pipeline

        :param target_api_config_path: Path to the target api config file
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        """
        # Create an ordered dict of all ingest stages and their parameters
        self.stage_dict = OrderedDict()

        # Extract stage
        self.stage_dict['e'] = (ExtractStage,
                                self.ingest_output_dir,
                                self.data_ingest_config.extract_config_paths)

        # Transform stage
        transform_fp = None
        # Create file path to transform function Python module
        if not auto_transform:
            transform_fp = self.data_ingest_config.transform_function_path
            if transform_fp:
                transform_fp = os.path.join(
                    self.ingest_config_dir, os.path.relpath(transform_fp))

        self.stage_dict['t'] = (TransformStage, target_api_config_path,
                                self.ingest_output_dir, transform_fp)

        # Load stage
        self.stage_dict['l'] = (
            LoadStage, target_api_config_path,
            target_url, use_async,
            self.data_ingest_config.target_service_entities)

        # Iterate over stages and execute them
        output = None
        for key, params in self.stage_dict.items():
            # Instantiate an instance of the ingest stage
            stage = params[0](*(params[1:]))
            # First stage is always extract
            if key == 'e':
                output = stage.run()
            else:
                output = stage.run(output)
