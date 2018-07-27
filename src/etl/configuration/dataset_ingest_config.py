import os
from etl.configuration.base_config import YamlConfig
from config import (
    DATASET_INGEST_CONFIG_DEFAULT_FILENAME,
    DATA_INGEST_SCHEMA_PATH
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

        :params config_filepath: Path to the data ingest config file
        """
        # Is config path a dir or file
        config_path = os.path.abspath(os.path.expanduser(config_path))
        if os.path.isdir(config_path):
            # If dir, create default config file path
            config_path = os.path.join(
                config_path,
                DATASET_INGEST_CONFIG_DEFAULT_FILENAME)

        super().__init__(config_path, schema_path=DATA_INGEST_SCHEMA_PATH)
        self._deserialize()

    def _deserialize(self):
        """
        Deserialize the dictionary of parameters to attributes of the
        DatasetIngestConfig class
        """
        # List of entity types that should be loaded into target service
        self.target_service_entities = self.contents.get(
            'target_service_entities')

        # Directory containg the extract config files
        self.extract_config_dir = os.path.join(
            os.path.dirname(self.config_filepath),
            self.contents.get('extract_config_dir'))

        # List of extract config file paths
        self.extract_config_paths = [os.path.join(self.extract_config_dir,
                                                  filename)
                                     for filename in os.listdir(
                                     self.extract_config_dir)
                                     if filename.endswith('.py')]
        # Log params
        for param in ['log_dir', 'log_level', 'overwrite_log']:
            if ('logging' in self.contents and
                    (param in self.contents['logging'])):
                setattr(self, param, self.contents['logging'][param])
            else:
                setattr(self, param, None)

        # Study parameters
        self.study = self.contents.get('study')

        # Investigator parameters
        self.investigator = self.contents.get('investigator')
