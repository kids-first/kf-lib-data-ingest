import os

import pytest

from conftest import TEST_DATA_DIR
from etl.extract.extract import ExtractStage


def test_extract():
    extract_config_path = os.path.join(
        TEST_DATA_DIR, 'test_study', 'extract_configs', 'example1.py'
    )
    es = ExtractStage([extract_config_path])
    df_out = es.run()
