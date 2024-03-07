""" Ingest Package Config """

from kf_lib_data_ingest.common.concept_schema import CONCEPT

# The list of entities that will be loaded into the target service
target_service_entities = [
    "family",
    "participant",
    "diagnosis",
    "phenotype",
    "outcome",
    "biospecimen",
    "read_group",
    "sequencing_experiment",
    "genomic_file",
    "biospecimen_genomic_file",
    "sequencing_experiment_genomic_file",
    "read_group_genomic_file",
]

# All paths are relative to the directory this file is in
extract_config_dir = "extract_configs"

transform_function_path = "transform_module.py"

# TODO - Replace these values with your own valid values!
study = "SD_ME0WME0W"

expected_counts = {
    CONCEPT.FAMILY: 3,
    CONCEPT.PARTICIPANT: 9,
    CONCEPT.BIOSPECIMEN: 16,
}
