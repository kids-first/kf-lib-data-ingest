import os
import logging

from kf_lib_data_ingest.common.misc import read_json

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, 'output')

logger = logging.getLogger(__name__)


def concept_discovery_dict(stage_type):
    """
    Load the concept discovery JSON file for a stage into a dict

    :param stage_type: one of [ExtractStage, TransformStage]
    :type stage_type: str

    :returns the concept discovery dict. See
    kf_lib_data_ingest.common.stage._postrun_concept_discovery for details on
    format of dict
    """
    data = None
    stage_types = {'ExtractStage', 'TransformStage'}
    if stage_type not in stage_types:
        logger.error(
            f'Invalid stage type: {stage_type}. Must be one of {stage_types}')
        return data

    fp = os.path.join(OUTPUT_DIR, f'{stage_type}_concept_discovery.json')
    if os.path.isfile(fp):
        data = read_json(fp)

    return data
