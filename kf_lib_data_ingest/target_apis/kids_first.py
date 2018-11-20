"""
Configuration module specifying how a target model maps to the standard model

This module is translated into an
etl.configuration.target_api_config.TargetAPIConfig object which is used by the
ingest transform stage to populate instances of target model concepts
(i.e. participants, diagnoses, etc) with data from the standard model before
those instances are loaded into the target service (i.e Kids First Dataservice)

See etl.configuration.target_api_config docstring for more details on
requirements for format and content.
"""

from etl.transform.standard_model.concept_schema import CONCEPT

target_service_entity_id = 'kf_id'

target_concepts = {
    'investigator': {
        'standard_concept': CONCEPT.INVESTIGATOR,
        'properties': {
            'external_id': CONCEPT.INVESTIGATOR.ID,
            'name': CONCEPT.INVESTIGATOR.NAME,
            'institution': CONCEPT.INVESTIGATOR.INSTITUTION
        },
        'endpoint': '/investigators'
    },
    'study': {
        'standard_concept': CONCEPT.STUDY,
        'links': {
            'investigator_id': CONCEPT.INVESTIGATOR.ID,
        },
        'properties': {
            'external_id': CONCEPT.STUDY.ID,
            'name': CONCEPT.STUDY.NAME,
            'short_name': CONCEPT.STUDY.SHORT_NAME,
            'version': CONCEPT.STUDY.VERSION,
            'data_access_authority': CONCEPT.STUDY.AUTHORITY,
            'release_status': CONCEPT.STUDY.RELEASE_STATUS,
            'attribution': CONCEPT.STUDY.ATTRIBUTION,
            'category': CONCEPT.STUDY.CATEGORY
        },
        'endpoint': '/studies'
    },
    'family': {
        'standard_concept': CONCEPT.FAMILY,
        'properties': {
            'external_id': CONCEPT.FAMILY.ID
        },
        'endpoint': '/families'
    },
    'participant': {
        'standard_concept': CONCEPT.PARTICIPANT,
        'links': {
            'family_id': CONCEPT.FAMILY.ID,
            'study_id': CONCEPT.STUDY.ID
        },
        'properties': {
            "is_proband": CONCEPT.PARTICIPANT.IS_PROBAND,
            "consent_type": CONCEPT.PARTICIPANT.CONSENT_TYPE,
            "ethnicity": CONCEPT.PARTICIPANT.ETHNICITY,
            "gender": CONCEPT.PARTICIPANT.GENDER,
            "race": CONCEPT.PARTICIPANT.RACE
        },
        'endpoint': '/participants'
    },
    'diagnosis': {
        'standard_concept': CONCEPT.DIAGNOSIS,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.ID
        },
        'properties': {
            "external_id": CONCEPT.DIAGNOSIS.ID,
            "age_at_event_days": CONCEPT.DIAGNOSIS.ID,
            "source_text_diagnosis": CONCEPT.DIAGNOSIS.NAME,
            "source_text_tumor_location": CONCEPT.DIAGNOSIS.TUMOR_LOCATION,
            "mondo_id_diagnosis": CONCEPT.DIAGNOSIS.MONDO_ID,
            "icd_id_diagnosis": CONCEPT.DIAGNOSIS.ICD_ID,
            "uberon_id_tumor_location":
            CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID,
            "ncit_id_diagnosis": CONCEPT.DIAGNOSIS.NCIT_ID,
            "spatial_descriptor": CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR,
            "diagnosis_category": CONCEPT.DIAGNOSIS.CATEGORY,
        },
        'endpoint': '/diagnoses'
    },
    'phenotype': {
        'standard_concept': CONCEPT.PHENOTYPE,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.ID
        },
        'properties': {
            "external_id": CONCEPT.PHENOTYPE.ID,
            "age_at_event_days": CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
            "source_text_phenotype": CONCEPT.PHENOTYPE.NAME,
            "hpo_id_phenotype": CONCEPT.PHENOTYPE.HPO_ID,
            "snomed_id_phenotype": CONCEPT.PHENOTYPE.SNOMED_ID,
            "observed": CONCEPT.PHENOTYPE.OBSERVED,
        },
        'endpoint': '/phenotypes'
    },
    'outcome': {
        'standard_concept': CONCEPT.OUTCOME,
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.ID
        },
        'properties': {
            "external_id": CONCEPT.OUTCOME.ID,
            "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
            "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
            "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
        },
        'endpoint': '/outcomes'
    },
    'biospecimen': {
        'standard_concept': CONCEPT.BIOSPECIMEN,
        'links': {
            'sequencing_center_id': CONCEPT.SEQUENCING.CENTER.ID,
            'participant_id': CONCEPT.PARTICIPANT.ID
        },
        'properties': {
            "external_sample_id": CONCEPT.BIOSPECIMEN.ID,
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
        },
        'endpoint': '/biospecimens'
    },
    'genomic_file': {
        'standard_concept': CONCEPT.GENOMIC_FILE,
        'links': {
            'sequencing_experiment': CONCEPT.SEQUENCING.ID
        },
        'properties': {
            "external_id": CONCEPT.GENOMIC_FILE.ID,
            "file_name": CONCEPT.GENOMIC_FILE.FILE_NAME,
            "file_format": None,
            "data_type": None,
            "availability": None,
            "controlled_access": None,
            "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
            "paired_end": CONCEPT.READ_GROUP.PAIRED_END,
            "hashes": CONCEPT.GENOMIC_FILE.ID,
            "size": CONCEPT.GENOMIC_FILE.ID,
            "urls": CONCEPT.GENOMIC_FILE.ID,
            "acl": CONCEPT.GENOMIC_FILE.ID,
            "reference_genome": CONCEPT.GENOMIC_FILE.ID
        },
        'endpoint': '/genomic-files'
    },
    'read_group': {
        'standard_concept': CONCEPT.READ_GROUP,
        'links': {
            'genomic_file': CONCEPT.GENOMIC_FILE.ID
        },
        'properties': {
            "external_id": CONCEPT.READ_GROUP.ID,
            "flow_cell": CONCEPT.READ_GROUP.FLOW_CELL,
            "lane_number": CONCEPT.READ_GROUP.LANE_NUMBER,
            "quality_scale": CONCEPT.READ_GROUP.QUALITY_SCALE
        },
        'endpoint': '/read-groups'
    },
    'sequencing_experiment': {
        'standard_concept': CONCEPT.SEQUENCING,
        'links': {
            'sequencing_center_id': CONCEPT.SEQUENCING.CENTER.ID
        },
        'properties': {
            "external_id": CONCEPT.SEQUENCING.ID,
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
            "mean_read_length": CONCEPT.SEQUENCING.MEAN_READ_LENGTH
        },
        'endpoint': '/sequencing-experiments'
    },
    'sequencing_center': {
        'standard_concept': CONCEPT.SEQUENCING.CENTER,
        'properties': {
            "external_id": CONCEPT.SEQUENCING.CENTER.ID,
            "NAME": CONCEPT.SEQUENCING.CENTER.NAME
        },
        'endpoint': '/sequencing-centers'
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
    CONCEPT.BIOSPECIMEN: {CONCEPT.GENOMIC_FILE},
    CONCEPT.SEQUENCING: {CONCEPT.GENOMIC_FILE},
    CONCEPT.SEQUENCING.CENTER: {
        CONCEPT.BIOSPECIMEN,
        CONCEPT.SEQUENCING
    }
}
