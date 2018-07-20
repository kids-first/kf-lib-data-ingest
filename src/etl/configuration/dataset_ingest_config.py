import os
from etl.configuration.base_config import YamlConfig
from config import (
    DATASET_INGEST_CONFIG_DEFAULT_FILENAME,
    DATA_INGEST_SCHEMA_PATH
)


class DatasetIngestConfig(YamlConfig):
    # TODO

    def __init__(self, config_path):
        # Is config path a dir or file
        if os.path.isdir(config_path):
            # If dir, create default config file path
            config_path = os.path.join(
                config_path,
                DATASET_INGEST_CONFIG_DEFAULT_FILENAME)

        super().__init__(config_path, schema=DATA_INGEST_SCHEMA_PATH)
        self._deserialize()

    def _deserialize(self):
        self.target_service_entities = self.contents.get(
            'target_service_entities')
        self.extract_config_dir = os.path.join(
            os.path.dirname(self.config_filepath),
            self.contents.get('extract_config_dir'))
        self.extract_config_paths = [os.path.join(self.extract_config_dir,
                                                  filename)
                                     for filename in os.listdir(
                                     self.extract_config_dir)
                                     if filename.endswith('.py')]
        self.study = self.contents.get('study')
        self.investigator = self.contents.get('investigator')
