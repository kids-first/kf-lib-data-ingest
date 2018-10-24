import inspect
import logging
from collections import OrderedDict

from common.misc import kwargs_from_frame
from etl.configuration.dataset_ingest_config import DatasetIngestConfig
from etl.configuration.log import setup_logger
from etl.extract.extract import ExtractStage
from etl.transform.transform import TransformStage
from etl.load.load import LoadStage
from config import INGEST_OP


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        """
        self.dataset_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)

        # Create ordered stages
        self.stages = OrderedDict()
        self.stages[ExtractStage.operation] = self.extract
        self.stages[TransformStage.operation] = self.transform
        self.stages[LoadStage.operation] = self.load

    def invoke(self, operation_str, *op_args, **op_kwargs):
        """
        Entry point for executing the ingest pipeline or a particular stage of
        the pipeline.

        Execute the operation in top level exception handler so that all
        exceptions are logged.

        :param operation_str: a string containing the operation to invoke
        (i.e ingest, extract, transform, load)
        :param op_args: the positional arguments expected by the <operation>
        method
        :param op_kwargs: the keyword arguments expected by the <operation>
        method
        """
        # Create logger
        self._create_logger()

        # Log the start of the run
        self.logger.info('BEGIN data ingestion')
        self._log_method_args(inspect.currentframe())

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:

            # Execute ingest or one of the ingest stages
            if operation_str == INGEST_OP:
                operation = self.run
            else:
                operation = self.stages.get(operation_str)

            operation(*op_args, **op_kwargs)

        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def run(self, target_api_config_path, target_url,
            use_async, output_dir, overwrite_output):
        """
        Runs the ingest pipeline

        :param target_api_config_path: Path to the target api config file
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded
        """
        # Log run method's arguments
        self._log_method_args(inspect.currentframe())

        output = self.extract(output_dir, overwrite_output)
        output = self.transform(output_dir, overwrite_output, input=output)
        self.load(target_api_config_path, target_url,
                  use_async, output_dir, overwrite_output, input=output)

    def extract(self, output_dir, overwrite_output):
        # Run extract
        extract_config_paths = self.dataset_ingest_config.extract_config_paths
        stage = ExtractStage(extract_config_paths)
        output = stage.run()

        # stage.write_output(output, overwrite_output, output_dir)

        return output

    def transform(self, overwrite_output, output_dir, input=None):
        # if not input:
        #     current_stage = self.stages.get(TransformStage.operation)
        #     previous_stage = the one before current
        stage = TransformStage()
        output = stage.run(input)

        # stage.write_output(output, overwrite_output, output_dir)

        return output

    def load(self, target_api_config_path, target_url, use_async,
             overwrite_output, output_dir, input=None):
        entities = self.dataset_ingest_config.target_service_entities
        stage = LoadStage(target_api_config_path, target_url,
                          use_async, entities)

        # if not input:
        #     current_stage = self.stages.get(TransformStage.operation)
        #     previous_stage = the one before current

        output = stage.run(input)

        # stage.write_output(output, overwrite_output, output_dir)

        return output

    def _create_logger(self):
        """
        Get log params from dataset_ingest_config

        :param dataset_ingest_config a DatasetIngestConfig object containing
        log parameters
        """
        # Get log dir
        log_dir = self.dataset_ingest_config.log_dir

        # Get optional log params
        opt_log_method_args = {
            param: getattr(self.dataset_ingest_config, param)
            for param in ['overwrite_log', 'log_level']
        }

        # Setup root logger
        setup_logger(log_dir, **opt_log_method_args)

        # Set class level logger
        self.logger = logging.getLogger(__name__)

    def _log_method_args(self, current_frame):
        """
        Log the arguments for the caller's stack frame

        :param current_frame: frame object for the caller’s stack frame
        """
        kwargs = kwargs_from_frame(current_frame)
        param_string = '\n\t'.join(['{} = {}'.format(key, value)
                                    for key, value in kwargs.items()])
        self.logger.info(f'Parameters:\n\t{param_string}')
