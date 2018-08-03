import os
import pytest

from conftest import TEST_DATA_DIR
from etl.ingest_pipeline import DataIngestPipeline


def test():
    target_api_config_path = os.path.join(TEST_DATA_DIR,
                                          'test_api_config.py')
    data_ingest_config_path = os.path.join(TEST_DATA_DIR,
                                           'test_study',
                                           'dataset_ingest_config.yml')
    p = DataIngestPipeline(data_ingest_config_path)

    p.run(target_api_config_path)
