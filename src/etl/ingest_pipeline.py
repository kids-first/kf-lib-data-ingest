from collections import OrderedDict

from etl.configuration.dataset_ingest_config import DatasetIngestConfig
from etl.extract.extract import ExtractStage
from etl.transform.transform import TransformStage
from etl.load.load import LoadStage

from config import (
    DATASET_INGEST_CONFIG_DEFAULT_FILENAME,
    DEFAULT_TARGET_URL,
    TARGET_SERVICE_CONFIG_PATH
)

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)

    def run(self, target_api_config_path, use_async=False,
            target_url=DEFAULT_TARGET_URL):

        self.stage_dict = OrderedDict()
        self.stage_dict['e'] = (ExtractStage,
                                self.data_ingest_config.extract_config_paths)
        self.stage_dict['t'] = (TransformStage, )

        self.stage_dict['l'] = (
            LoadStage, target_api_config_path,
            target_url, use_async,
            self.data_ingest_config.target_service_entities)

        output = None
        for key, params in self.stage_dict.items():
            stage = params[0](*(params[1:]))
            if key == 'e':
                output = stage.run()
            else:
                output = stage.run(output)
