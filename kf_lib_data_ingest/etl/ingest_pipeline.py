import inspect
import logging
import os
from pprint import pformat
from collections import OrderedDict

from kf_lib_data_ingest.config import DEFAULT_TARGET_URL
from kf_lib_data_ingest.etl.configuration.dataset_ingest_config import (
    DatasetIngestConfig,
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage
from kf_lib_data_ingest.common.misc import kwargs_from_frame
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
        self._make_output_dir()

    def run(self, target_api_config_path, auto_transform=False,
            use_async=False, target_url=DEFAULT_TARGET_URL,
            write_output=False):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.

        :param target_api_config_path: Path to the target api config file
        :param auto_transform: Specifies whether to use auto-transformation
        or guided transformation
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        :param write_output: Specifies whether to write stage output to file
        """
        # Create logger
        self._get_log_params(self.data_ingest_config)
        self.logger = logging.getLogger(__name__)

        # Log the start of the run with ingestion parameters
        kwargs = kwargs_from_frame(inspect.currentframe())
        run_msg = ('BEGIN data ingestion.\n\t-- Ingestion params --\n'
                   f'{pformat(kwargs)}')
        self.logger.info(run_msg)

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            self._run(**kwargs)
        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def _make_output_dir(self):
        """
        Make top level ingest output dir
        """
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath)
        self.ingest_output_dir = os.path.join(self.ingest_config_dir,
                                              'output_cache')
        os.makedirs(self.ingest_output_dir, exist_ok=True)

    def _get_log_params(self, data_ingest_config):
        """
        Get log params from data_ingest_config

        :param data_ingest_config a DatasetIngestConfig object containing
        log parameters
        """
        # Get log dir
        log_dir = data_ingest_config.log_dir

        # Get optional log params
        opt_log_params = {param: getattr(data_ingest_config, param)
                          for param in ['overwrite_log', 'log_level']}

        # Setup logger
        setup_logger(log_dir, **opt_log_params)

    def _run(self, **kwargs):
        """
        Runs the ingest pipeline

        See run method for kwargs description
        """
        # Create an ordered dict of all ingest stages and their parameters
        self.stage_dict = OrderedDict()

        # Get common stage params
        target_api_config_path = kwargs.get('target_api_config_path')

        # Extract stage
        self.stage_dict[ExtractStage] = {
            'args': [(self.data_ingest_config.extract_config_paths)],
            'kwargs': {
                'ingest_output_dir': self.ingest_output_dir,
                'write_output': kwargs.get('write_output')
            }
        }

        # Transform stage
        transform_fp = None
        # Create file path to transform function Python module
        if not kwargs.get('auto_transform'):
            transform_fp = self.data_ingest_config.transform_function_path
            if transform_fp:
                transform_fp = os.path.join(
                    self.ingest_config_dir, os.path.relpath(transform_fp))

        self.stage_dict[TransformStage] = {
            'args': [target_api_config_path],
            'kwargs': {
                'ingest_output_dir': self.ingest_output_dir,
                'write_output': kwargs.get('write_output'),
                'transform_function_path': transform_fp,
            }
        }

        # Load stage
        self.stage_dict[LoadStage] = {
            'args': [target_api_config_path, kwargs.get('target_url')],
            'kwargs': {
                'ingest_output_dir': self.ingest_output_dir,
                'write_output': kwargs.get('write_output'),
                'use_async': kwargs.get('use_async'),
                'entities_to_load':
                self.data_ingest_config.target_service_entities,
            }
        }
        # Iterate over stages and execute them
        output = None
        for stage_cls, params in self.stage_dict.items():
            # Instantiate an instance of the ingest stage
            stage = stage_cls(*(params['args']), **params['kwargs'])
        # First stage is always extract
            if stage_cls.__name__ == 'ExtractStage':
                output = stage.run()
            else:
                output = stage.run(output)
