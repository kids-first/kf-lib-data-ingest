""" Dataset Ingest Config """
from kf_lib_data_ingest.common.concept_schema import CONCEPT

# The list of entities that will be loaded into the target service
target_service_entities = [
    # 'study_file',  # not ready
    # 'family',  # don't have any
    'participant',
    # 'family_relationship',  # dont have any
    # 'diagnosis',  # dont have any
    'phenotype',
    'outcome',
    'biospecimen',
    'sequencing_experiment',
    'genomic_file',
]

overwrite_log = True
log_level = 'info'

# All extract config paths are relative to the directory this file is in
extract_config_dir = 'extract_configs'

transform_function_path = 'transform_module.py'

expected_counts = {
  CONCEPT.BIOSPECIMEN: 60,
  CONCEPT.PARTICIPANT.ID: 25
}

study = 'SD_ME0WME0W'
