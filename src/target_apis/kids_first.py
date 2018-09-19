
# Target Service Config
# TODO

# Target Model Schema
target_model_schema = {}

# RELATIONSHIPS = {
#     CONCEPT.FAMILY: {CONCEPT.PARTICIPANT},
#     CONCEPT.PARTICIPANT: {CONCEPT.BIOSPECIMEN,
#                           CONCEPT.DIAGNOSIS,
#                           CONCEPT.PHENOTYPE,
#                           CONCEPT.OUTCOME},
#     CONCEPT.BIOSPECIMEN: {CONCEPT.GENOMIC_FILE},
#     CONCEPT.SEQUENCING: {CONCEPT.GENOMIC_FILE}
# }

# Contains the mapping of standard model to target model (dataservice)
# Include concept to entity mapping, property to property mapping

# The source data ids will be used as keys in the ID cache.
# ID cache contains a dict where the keys are the source data IDs and
# the values are target service IDs for created entities.

# Name of the source data ID key (external_id)
source_data_id_name = 'external_id'
# Name of the target model ID or primary key name (kf_id)
target_model_id_name = 'kf_id'

# Transport configuration parameters
# Kids First entities to endpoint mapping
entity_endpoint_map = {
    'study': '/studies',
    'investigator': '/investigators',
    'study_file': '/study-files',
    'family': '/families',
    'family_relationship': '/family-relationships',
    'cavatica_app': '/cavatica-apps',
    'sequencing_center': '/sequencing-centers',
    'participant': '/participants',
    'diagnosis': '/diagnoses',
    'phenotype': '/phenotypes',
    'outcome': '/outcomes',
    'biospecimen': '/biospecimens',
    'genomic_file': '/genomic-files',
    'read_group': '/read-groups',
    'sequencing_experiment': '/sequencing-experiments',
    'cavatica_fask': '/cavatica-tasks',
    'cavatica_task_genomic_file': '/cavatica-task-genomic-files'
}
# Contains the config for the transport mechanism (HTTP, DB, etc)
