from etl.configuration.base_config import YamlConfig


class DatasetIngestConfig(YamlConfig):
    # TODO

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extract_config_paths = self._config_file.get('', [])
