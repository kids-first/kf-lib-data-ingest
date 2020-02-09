"""
Auto-generated conftest module

Contains helper methods or common dependencies for all user defined tests

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing data validation tests.
"""

import logging
import os

from kf_lib_data_ingest.common.io import read_json

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")

logger = logging.getLogger(__name__)


def concept_discovery_dict(stage_type):
    """
    Load a stage's concept discovery JSON structure into a dict.

    The concept discovery dict can be used to write tests for verifying counts
    of concepts, counts of concept attributes or counts of relationships
    between concepts.

    See kf_lib_data_ingest.common.stage._postrun_concept_discovery for
    details on content and format of concept discovery data

    :param stage_type: one of [ExtractStage, TransformStage]
    :type stage_type: str

    :returns the concept discovery dict for a particular stage
    """
    fp = os.path.join(OUTPUT_DIR, f"{stage_type}_concept_discovery.json")
    assert os.path.isfile(fp)
    data = read_json(fp)

    return data
