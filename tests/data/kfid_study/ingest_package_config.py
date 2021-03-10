""" Dataset Ingest Config """
# The list of entities that will be loaded into the target service
target_service_entities = [
    "genomic_file",
    "biospecimen_genomic_file",
]

# All paths are relative to the directory this file is in
extract_config_dir = "extract_configs"

transform_function_path = "transform_module.py"

project = "SD_ME0WME0W"
