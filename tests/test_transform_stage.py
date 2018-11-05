"""
We only need to test validate run parameters since the actual transformation
functionality is heavily tested in tests/test_standard_model.py
"""

import pytest

import pandas as pd

from etl.transform.transform import TransformStage
from common.errors import InvalidIngestStageParameters


def test_invalid_run_parameters():

    stage = TransformStage()

    # Bad keys
    with pytest.raises(InvalidIngestStageParameters):
        stage.run({i: 'foo' for i in range(5)})

    # Bad values
    with pytest.raises(InvalidIngestStageParameters):
        stage.run({'foor': ('bar', None) for i in range(5)})
