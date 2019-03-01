import os

from kf_lib_data_ingest.etl.configuration.base_config import YamlConfig
from kf_lib_data_ingest.config import (
    DATASET_INGEST_CONFIG_DEFAULT_FILENAME,
    DATA_INGEST_SCHEMA_PATH,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT
)


class DatasetIngestConfig(YamlConfig):
    # TODO

    # Removal/Exclusion list
    # We periodically get messages from investigators saying that we should
    # remove/exclude certain items from the dataservice.
    # ( e.g. https://github.com/kids-first/kf-study-imports/
    # issues/75#issuecomment-405245066 )
    # We should be able to store a list of either external IDs and
    # their types or KFIDs of things to delete from the dataservice if already
    # there and to prevent from loading subsequently.

    def __init__(self, config_path):
        """
        Construct a DatasetIngestConfig object from a config file

        :param config_filepath: Path to the data ingest config file
        """
        # Is config path a dir or file
        config_path = os.path.abspath(os.path.expanduser(config_path))
        if os.path.isdir(config_path):
            # If dir, create default config file path
            config_path = os.path.join(
                config_path,
                DATASET_INGEST_CONFIG_DEFAULT_FILENAME)

        super().__init__(config_path, schema_path=DATA_INGEST_SCHEMA_PATH)

        # Directory containing the extract config files
        self.extract_config_dir = os.path.join(
            os.path.dirname(self.config_filepath),
            self.contents.get('extract_config_dir'))
        self.extract_config_paths = [os.path.join(self.extract_config_dir,
                                                  filename)
                                     for filename in os.listdir(
                                     self.extract_config_dir)
                                     if filename.endswith('.py')]
        self._set_log_params()
        self.transform_function_path = self.contents.get(
            'transform_function_path')
        self.study = self.contents.get('study')
        self.investigator = self.contents.get('investigator')
        self.target_service_entities = self.contents.get(
            'target_service_entities')

    def _set_log_params(self):
        """
        Set log parameters
        """

        def _default_log_dir():
            """
            Create default log directory
            """
            config_dir = os.path.abspath(
                os.path.dirname(self.config_filepath))
            log_dir = os.path.join(config_dir, 'logs')
            if not os.path.isdir(log_dir):
                os.mkdir(log_dir)
            return log_dir

        # Log params with defaults
        log_params = {
            'log_dir': _default_log_dir,
            'log_level': DEFAULT_LOG_LEVEL,
            'overwrite_log': DEFAULT_LOG_OVERWRITE_OPT
        }

        # Set values
        for param, default_value in log_params.items():
            # User supplied log param through config
            if ('logging' in self.contents and
                    (param in self.contents['logging'])):
                value = self.contents['logging'][param]
                setattr(self, param, value)
            # Set default
            else:
                if callable(default_value):
                    setattr(self, param, default_value())
                else:
                    setattr(self, param, default_value)
