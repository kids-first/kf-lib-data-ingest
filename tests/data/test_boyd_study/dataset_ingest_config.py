""" Dataset Ingest Config """
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# The list of entities that will be loaded into the target service
target_service_entities = [
    'study_file',
    'family',
    'participant',
    'family_relationship',
    'diagnosis',
    'phenotype',
    'outcome',
    'biospecimen',
    'sequencing_experiment',
    'genomic_file'
]

# All extract config paths are relative to the directory this file is in
extract_config_dir = 'extract_configs'

# Used by GuidedTransformStage
transform_function_path = 'transform_module.py'

log_level = 'debug'

study = 'SD_P445ACHV'
