"""
Auto-generated conftest module

Contains helper methods or common dependencies for all user defined tests

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing data validation tests.
"""

import logging
import os

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")

logger = logging.getLogger(__name__)


def load_stage_output(stage_type):
    """
    Load a stage's output DataFrames.

    The DataFrames can be used to write tests for verifying counts
    of concepts, counts of concept attributes or counts of relationships
    between concepts.

    :param stage_type: one of [ExtractStage, TransformStage]
    :type stage_type: str

    :returns the DataFrame(s) for a particular stage
    """
    # TODO
    pass
