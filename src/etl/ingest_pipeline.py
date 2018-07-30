import logging
from collections import OrderedDict

from etl.configuration.dataset_ingest_config import DatasetIngestConfig
from etl.extract.extract import ExtractStage
from etl.transform.transform import TransformStage
from etl.load.load import LoadStage
from config import (
    DEFAULT_TARGET_URL,
    setup_logger
)

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :params dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        """
        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)

    def run(self, target_api_config_path, use_async=False,
            target_url=DEFAULT_TARGET_URL):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.

        See _run method for param description
        """
        # Create logger
        self._setup_logging(self.data_ingest_config)
        self.logger = logging.getLogger(__name__)

        # Log the start of the run with ingesttion parameters
        import inspect
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        param_string = '\n\t'.join(['{} = {}'.format(arg, values[arg])
                                    for arg in args[1:]])
        run_msg = ('BEGIN data ingestion.\n\t-- Ingestion params --\n\t{}'
                   .format(param_string))
        self.logger.info(run_msg)

        # Top level exception handler
        # Catch exception, log it, and exit
        try:
            self._run(target_api_config_path, use_async, target_url)
        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def _setup_logging(self, data_ingest_config):
        """
        Setup logging

        :params data_ingest_config a DatasetIngestConfig object containing
        log parameters
        """
        # Get log dir
        log_dir = data_ingest_config.log_dir

        # Get optional log params
        opt_log_params = {param: getattr(data_ingest_config, param)
                          for param in ['overwrite_log', 'log_level']}

        # Setup logger
        setup_logger(log_dir, **opt_log_params)

    def _run(self, target_api_config_path, use_async=False,
             target_url=DEFAULT_TARGET_URL):
        """
        Runs the ingest pipeline

        :params target_api_config_path: Path to the target api config file
        :params use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :params target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        """
        # Create an ordered dict of all ingest stages and their parameters
        self.stage_dict = OrderedDict()
        self.stage_dict['e'] = (ExtractStage,
                                self.data_ingest_config.extract_config_paths)
        self.stage_dict['t'] = (TransformStage, )

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
