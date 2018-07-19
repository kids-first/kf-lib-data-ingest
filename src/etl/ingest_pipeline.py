from etl.extract.extract import Extractor
from etl.transform.transform import Transformer
from etl.load.load import Loader

from config import (
    DATASET_INGEST_CONFIG_DEFAULT_FILENAME,
    USE_ASYNC_KEY,
    TARGET_URL_KEY,
    DEFAULT_TARGET_URL,
    TARGET_SERVICE_CONFIG_PATH
)
# TODO


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path, stages, **kwargs):
        # TODO
        # dataset_ingest_config_path could be either a filepath or dirpath

        # If filepath - use filepath as is
        # else if is dirpath then look for a file called
        # DATASET_INGEST_CONFIG_DEFAULT_FILENAME.

        # Validate that the study config file exists

        # Also validate other CLI args which would be in kwargs
        use_async = kwargs.get(USE_ASYNC_KEY, False)
        target_url = kwargs.get(TARGET_URL_KEY, DEFAULT_TARGET_URL)

        stages = [Extractor(dataset_ingest_config_path),
                  Transformer(dataset_ingest_config_path),
                  Loader(target_url, use_async,
                         dataset_ingest_config_path,
                         TARGET_SERVICE_CONFIG_PATH)]

        # TODO
        output = 'whatever the extract stage needs'
        for stage in stages:
            output = stage.run(**output)
