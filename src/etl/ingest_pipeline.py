import inspect
import logging
from collections import OrderedDict

from etl.configuration.dataset_ingest_config import DatasetIngestConfig
from etl.configuration.log import setup_logger
from etl.extract.extract import ExtractStage
from etl.transform.transform import TransformStage
from etl.load.load import LoadStage
from config import DEFAULT_TARGET_URL


from pprint import pprint


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        """
        self.dataset_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)

        stages = OrderedDict()
        stages[ExtractStage.operation] = self.extract
        stages[TransformStage.operation] = self.transform
        stages[LoadStage.operation] = self.load

    def run(self, operation_str, *op_args, **op_kwargs):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.

        See _run method for param description
        """
        # Create logger
        self._get_log_params()
        self.logger = logging.getLogger(__name__)

        # Log the start of the run with ingestion parameters
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        param_string = '\n\t'.join(['{} = {}'.format(arg, values[arg])
                                    for arg in args[1:]])
        run_msg = ('BEGIN data ingestion.\n\t-- Run params --\n\t{}'
                   .format(param_string))
        self.logger.info(run_msg)

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            if operation_str == 'ingest':
                operation = self.ingest
            else:
                operation = self.stages.get(operation_str)
            operation(*op_args, **op_kwargs)
        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def _get_log_params(self):
        """
        Get log params from dataset_ingest_config

        :param dataset_ingest_config a DatasetIngestConfig object containing
        log parameters
        """
        # Get log dir
        log_dir = self.dataset_ingest_config.log_dir

        # Get optional log params
        opt_log_params = {param: getattr(self.dataset_ingest_config, param)
                          for param in ['overwrite_log', 'log_level']}

        # Setup logger
        setup_logger(log_dir, **opt_log_params)

    def ingest(self, target_api_config_path, use_async=False,
               target_url=DEFAULT_TARGET_URL):
        """
        Runs the ingest pipeline

        :param target_api_config_path: Path to the target api config file
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        """
        extract_config_paths = self.dataset_ingest_config.extract_config_paths
        output = ExtractStage(extract_config_paths).run()

        output = TransformStage().run(output)

        entities = self.dataset_ingest_config.target_service_entities
        output = LoadStage(target_api_config_path, target_url,
                           use_async, entities).run(output)

    # TODO each of these methods should prob be responsible for creating
    # stage, then reading the input, and running the stage

    # TODO Replace *args, and **kwargs with actual params

    # TODO May have to change some CLI opts to args, need to make sure
    # required CLI args match stage args, and opts match kwargs

    # TODO Rename the serialize/deserialize stage methods to read/write

    def extract(self, *args, **kwargs):
        # Run extract
        extract_config_paths = self.dataset_ingest_config.extract_config_paths
        stage = ExtractStage(extract_config_paths)
        output = stage.run()

        overwrite = kwargs.get('overwrite')
        output_dir = kwargs.get('output_dir')
        stage.write_output(output, overwrite, output_dir)

    def transform(self, *args, **kwargs):
        pass

    def load(self, *args, **kwargs):
        pass
