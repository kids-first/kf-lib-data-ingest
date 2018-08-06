import os

from conftest import TEST_DATA_DIR


def test(ingest_pipeline):
    target_api_config_path = os.path.join(TEST_DATA_DIR,
                                          'test_api_config.py')

    ingest_pipeline.run(target_api_config_path)
