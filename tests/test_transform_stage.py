"""
We only need to test validate run parameters since the actual transformation
functionality is heavily tested in tests/test_standard_model.py
"""
import os

import pytest
import pandas as pd

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters


def test_invalid_run_parameters(transform_stage):

    # Bad keys
    with pytest.raises(InvalidIngestStageParameters):
        transform_stage.run({i: 'foo' for i in range(5)})

    # Bad values
    with pytest.raises(InvalidIngestStageParameters):
        transform_stage.run({'foor': ('bar', None) for i in range(5)})
