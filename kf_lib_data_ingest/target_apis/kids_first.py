"""
Configuration module specifying how a target model maps to the standard model

This module is translated into an
etl.configuration.target_api_config.TargetAPIConfig object which is used by the
transform stage to populate the concept graph and by the load stage to populate
instances of target model concepts (i.e. participants, diagnoses, etc)
with data from the standard model before those instances are loaded into the
target service (i.e Kids First Dataservice)

See etl.configuration.target_api_config docstring for more details on
requirements for format and content.
"""
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT
)

target_service_entity_id = 'kf_id'

target_concepts = {
    'investigator': {
        'standard_concept': CONCEPT.INVESTIGATOR,
        'properties': {
            'external_id': CONCEPT.INVESTIGATOR.UNIQUE_KEY,
            'name': CONCEPT.INVESTIGATOR.NAME,
            'institution': CONCEPT.INVESTIGATOR.INSTITUTION,
            'visible': CONCEPT.INVESTIGATOR.VISIBLE
        },
        'endpoint': '/investigators'
    },
    'study': {
        'standard_concept': CONCEPT.STUDY,
        'links': {
            'investigator_id': CONCEPT.INVESTIGATOR.UNIQUE_KEY,
        },
        'properties': {
            'external_id': CONCEPT.STUDY.UNIQUE_KEY,
            'name': CONCEPT.STUDY.NAME,
            'short_name': CONCEPT.STUDY.SHORT_NAME,
            'version': CONCEPT.STUDY.VERSION,
            'data_access_authority': CONCEPT.STUDY.AUTHORITY,
            'release_status': CONCEPT.STUDY.RELEASE_STATUS,
            'attribution': CONCEPT.STUDY.ATTRIBUTION,
            'category': CONCEPT.STUDY.CATEGORY,
            'visible': CONCEPT.STUDY.VISIBLE
        },
        'endpoint': '/studies'
    },
    'family': {
        'standard_concept': CONCEPT.FAMILY,
        'properties': {
            'external_id': CONCEPT.FAMILY.UNIQUE_KEY,
            'visible': CONCEPT.FAMILY.VISIBLE
        },
        'endpoint': '/families'
    },
    'participant': {
        'standard_concept': CONCEPT.PARTICIPANT,
        'links': {
            'family_id': CONCEPT.FAMILY.UNIQUE_KEY,
            'study_id': CONCEPT.STUDY.UNIQUE_KEY
        },
        'properties': {
            'external_id': CONCEPT.PARTICIPANT.UNIQUE_KEY,
            "is_proband": CONCEPT.PARTICIPANT.IS_PROBAND,
            "ethnicity": CONCEPT.PARTICIPANT.ETHNICITY,
            "gender": CONCEPT.PARTICIPANT.GENDER,
            "race": CONCEPT.PARTICIPANT.RACE,
            'visible': CONCEPT.PARTICIPANT.VISIBLE,
            "affected_status": CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY
        },
        'endpoint': '/participants'
    },
    'diagnosis': {
        'standard_concept': CONCEPT.DIAGNOSIS,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.DIAGNOSIS.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.DIAGNOSIS.UNIQUE_KEY,
            "source_text_diagnosis": CONCEPT.DIAGNOSIS.NAME,
            "source_text_tumor_location": CONCEPT.DIAGNOSIS.TUMOR_LOCATION,
            "mondo_id_diagnosis": CONCEPT.DIAGNOSIS.MONDO_ID,
            "icd_id_diagnosis": CONCEPT.DIAGNOSIS.ICD_ID,
            "uberon_id_tumor_location":
            CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID,
            "ncit_id_diagnosis": CONCEPT.DIAGNOSIS.NCIT_ID,
            "spatial_descriptor": CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR,
            "diagnosis_category": CONCEPT.DIAGNOSIS.CATEGORY,
            'visible': CONCEPT.DIAGNOSIS.VISIBLE
        },
        'endpoint': '/diagnoses'
    },
    'phenotype': {
        'standard_concept': CONCEPT.PHENOTYPE,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.PHENOTYPE.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
            "source_text_phenotype": CONCEPT.PHENOTYPE.NAME,
            "hpo_id_phenotype": CONCEPT.PHENOTYPE.HPO_ID,
            "snomed_id_phenotype": CONCEPT.PHENOTYPE.SNOMED_ID,
            "observed": CONCEPT.PHENOTYPE.OBSERVED,
            'visible': CONCEPT.PHENOTYPE.VISIBLE
        },
        'endpoint': '/phenotypes'
    },
    'outcome': {
        'standard_concept': CONCEPT.OUTCOME,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.OUTCOME.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
            "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
            "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
            'visible': CONCEPT.OUTCOME.VISIBLE
        },
        'endpoint': '/outcomes'
    },
    'biospecimen': {
        'standard_concept': CONCEPT.BIOSPECIMEN,
        'links': {
            'sequencing_center_id': CONCEPT.SEQUENCING.CENTER.UNIQUE_KEY,
            'participant_id': CONCEPT.PARTICIPANT.UNIQUE_KEY
        },
        'properties': {
            "external_sample_id": CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
            'external_aliquot_id': CONCEPT.BIOSPECIMEN.ALIQUOT_ID,
            "source_text_tissue_type": CONCEPT.BIOSPECIMEN.TISSUE_TYPE,
            "composition": CONCEPT.BIOSPECIMEN.COMPOSITION,
            "source_text_anatomical_site":
            CONCEPT.BIOSPECIMEN.ANATOMY_SITE,
            "age_at_event_days": CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS,
            "source_text_tumor_descriptor":
            CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR,
            "ncit_id_tissue_type": CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID,
            "ncit_id_anatomical_site":
            CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID,
            "spatial_descriptor": CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR,
            "shipment_origin": CONCEPT.BIOSPECIMEN.SHIPMENT_ORIGIN,
            "shipment_date": CONCEPT.BIOSPECIMEN.SHIPMENT_DATE,
            "analyte_type": CONCEPT.BIOSPECIMEN.ANALYTE,
            "concentration_mg_per_ml":
            CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML,
            "volume_ml": CONCEPT.BIOSPECIMEN.VOLUME_ML,
            'visible': CONCEPT.BIOSPECIMEN.VISIBLE
        },
        'endpoint': '/biospecimens'
    },
    'genomic_file': {
        'standard_concept': CONCEPT.GENOMIC_FILE,
        'properties': {
            "external_id": CONCEPT.GENOMIC_FILE.UNIQUE_KEY,
            "file_name": CONCEPT.GENOMIC_FILE.FILE_NAME,
            "file_format": CONCEPT.GENOMIC_FILE.FILE_FORMAT,
            "data_type": CONCEPT.GENOMIC_FILE.DATA_TYPE,
            "availability": CONCEPT.GENOMIC_FILE.AVAILABILITY,
            "controlled_access": None,
            "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
            "hashes": CONCEPT.GENOMIC_FILE.HASH,
            "size": CONCEPT.GENOMIC_FILE.SIZE,
            "urls": CONCEPT.GENOMIC_FILE.URL,
            "acl": None,
            "reference_genome": CONCEPT.GENOMIC_FILE.REFERENCE_GENOME,
            'visible': CONCEPT.GENOMIC_FILE.VISIBLE
        },
        'endpoint': '/genomic-files'
    },
    'read_group': {
        'standard_concept': CONCEPT.READ_GROUP,
        'links': {
            'genomic_file_id': CONCEPT.GENOMIC_FILE.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.READ_GROUP.UNIQUE_KEY,
            "flow_cell": CONCEPT.READ_GROUP.FLOW_CELL,
            "lane_number": CONCEPT.READ_GROUP.LANE_NUMBER,
            "quality_scale": CONCEPT.READ_GROUP.QUALITY_SCALE,
            'visible': CONCEPT.READ_GROUP.VISIBLE
        },
        'endpoint': '/read-groups'
    },
    'sequencing_experiment': {
        'standard_concept': CONCEPT.SEQUENCING,
        'links': {
            'sequencing_center_id': CONCEPT.SEQUENCING.CENTER.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.SEQUENCING.UNIQUE_KEY,
            "experiment_date": CONCEPT.SEQUENCING.DATE,
            "experiment_strategy": CONCEPT.SEQUENCING.STRATEGY,
            "library_name": CONCEPT.SEQUENCING.LIBRARY_NAME,
            "library_strand": CONCEPT.SEQUENCING.LIBRARY_STRAND,
            "is_paired_end": CONCEPT.SEQUENCING.PAIRED_END,
            "platform": CONCEPT.SEQUENCING.PLATFORM,
            "instrument_model": CONCEPT.SEQUENCING.INSTRUMENT,
            "max_insert_size": CONCEPT.SEQUENCING.MAX_INSERT_SIZE,
            "mean_insert_size": CONCEPT.SEQUENCING.MEAN_INSERT_SIZE,
            "mean_depth": CONCEPT.SEQUENCING.MEAN_DEPTH,
            "total_reads": CONCEPT.SEQUENCING.TOTAL_READS,
            "mean_read_length": CONCEPT.SEQUENCING.MEAN_READ_LENGTH,
            'visible': CONCEPT.SEQUENCING.VISIBLE
        },
        'endpoint': '/sequencing-experiments'
    },
    'sequencing_center': {
        'standard_concept': CONCEPT.SEQUENCING.CENTER,
        'properties': {
            "external_id": CONCEPT.SEQUENCING.CENTER.UNIQUE_KEY,
            "name": CONCEPT.SEQUENCING.CENTER.NAME,
            'visible': CONCEPT.SEQUENCING.CENTER.VISIBLE
        },
        'endpoint': '/sequencing-centers'
    },
    'biospecimen_genomic_file': {
        'standard_concept': CONCEPT.BIOSPECIMEN_GENOMIC_FILE,
        'links': {
            'biospecimen_id': CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
            'genomic_file_id': CONCEPT.GENOMIC_FILE.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY,
            'visible': CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE
        },
        'endpoint': '/biospecimen-genomic-files'
    },
    'biospecimen_diagnosis': {
        'standard_concept': CONCEPT.BIOSPECIMEN_DIAGNOSIS,
        'links': {
            'biospecimen_id': CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
            'diagnosis_id': CONCEPT.DIAGNOSIS.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.UNIQUE_KEY,
            'visible': CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE
        },
        'endpoint': '/biospecimen-diagnoses'
    },
    'read_group_genomic_file': {
        'standard_concept': CONCEPT.READ_GROUP_GENOMIC_FILE,
        'links': {
            'read_group_id': CONCEPT.READ_GROUP.UNIQUE_KEY,
            'genomic_file_id': CONCEPT.GENOMIC_FILE.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.READ_GROUP_GENOMIC_FILE.UNIQUE_KEY,
            'visible': CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBLE
        },
        'endpoint': '/read-group-genomic-files'
    },
    'sequencing_experiment_genomic_file': {
        'standard_concept': CONCEPT.SEQUENCING_GENOMIC_FILE,
        'links': {
            'sequencing_experiment_id': CONCEPT.SEQUENCING.UNIQUE_KEY,
            'genomic_file_id': CONCEPT.GENOMIC_FILE.UNIQUE_KEY
        },
        'properties': {
            "external_id": CONCEPT.SEQUENCING_GENOMIC_FILE.UNIQUE_KEY,
            'visible': CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE
        },
        'endpoint': '/sequencing-experiment-genomic-files'
    }
}

relationships = {
    CONCEPT.INVESTIGATOR: {CONCEPT.STUDY},
    CONCEPT.STUDY: {CONCEPT.PARTICIPANT},
    CONCEPT.FAMILY: {CONCEPT.PARTICIPANT},
    CONCEPT.PARTICIPANT: {CONCEPT.BIOSPECIMEN,
                          CONCEPT.DIAGNOSIS,
                          CONCEPT.PHENOTYPE,
                          CONCEPT.OUTCOME},
    CONCEPT.DIAGNOSIS: {CONCEPT.BIOSPECIMEN_DIAGNOSIS},
    CONCEPT.BIOSPECIMEN: {CONCEPT.BIOSPECIMEN_GENOMIC_FILE,
                          CONCEPT.BIOSPECIMEN_DIAGNOSIS},
    CONCEPT.GENOMIC_FILE: {CONCEPT.BIOSPECIMEN_GENOMIC_FILE,
                           CONCEPT.READ_GROUP_GENOMIC_FILE,
                           CONCEPT.SEQUENCING_GENOMIC_FILE},
    CONCEPT.READ_GROUP: {CONCEPT.READ_GROUP_GENOMIC_FILE},
    CONCEPT.SEQUENCING: {CONCEPT.GENOMIC_FILE,
                         CONCEPT.SEQUENCING_GENOMIC_FILE},
    CONCEPT.SEQUENCING.CENTER: {
        CONCEPT.BIOSPECIMEN,
        CONCEPT.SEQUENCING
    }
}
