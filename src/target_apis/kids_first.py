from etl.transform.standard_model.concept_schema import CONCEPT

"""
Configuration module specifying how a target model maps to the standard model
This module is used by the transform stage to populate instances of target
model concepts (i.e. participants, diagnoses, etc) with data from the standard
model before those instances are loaded into the target service
(i.e. Kids First data service).

This module must have the following required attributes:

    - target_concepts
    - relationships
    - endpoints

These required attributes must be dicts.

    - target_concepts
        A dict of dicts. Each inner dict is a target concept dict, which
        contains mappings of target concept properties to standard concept
        attributes.

        A target concept dict has the following schema:

            Required Keys:
                - id
                - properties
            Optional Keys:
                - links

        {
            'id': {
                <target concept identifier property>:
                    <standard concept attribute>
            },
            'properties': {
                '<target concept property>': <standard concept attribute>,
                ...
            },
            links: {
                <target concept property>: <standard concept>
            }
        }

        It may seem unecessary to separate the mappings into
        'id', 'properties', and 'links', but this is important because the
        mappings in 'id' and 'links' are treated differently than those in
        'properties'. The value in a key, value pair under 'id' or 'links',
        during the transform stage, will be looked up in the standard model,
        and then during the load stage be translated into a target model ID.
        A value in a key, value pair under 'properties', during the transform
        stage, will be looked up in the standard model and then kept as is
        during the load stage.

        For example, after transformation and before loading, the 'id' dict
        could be:

            'id': {
                'kf_id': CONCEPT|PARTICIPANT|ID|P1
            }
        And during the loading stage the 'id' dict will be translated into:
            'id': {
                'kf_id': PT_00001111
            }

    - relationships
        A dict of sets which represents an adjacency list. The adjacency list
        codifies the parent-child relationships between target concepts. A
        key in the relationships dict should be a string containing a parent
        target concept, and the values should be a set of child target
        concepts.

    - endpoints
        A dict of key value pairs where the key should be a target concept
        and the value should be an endpoint in the target service to perform
        create, read, update, and delete operations for that target concept.

"""

target_concepts = {
    'investigator': {
        'id': {
            'kf_id': CONCEPT.INVESTIGATOR.ID
        },
        'properties': {
            'external_id': CONCEPT.INVESTIGATOR.ID,
            'kf_id': CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            'name': CONCEPT.INVESTIGATOR.NAME,
            'institution': CONCEPT.INVESTIGATOR.INSTITUTION
        }
    },
    'study': {
        'id': {
            'kf_id': CONCEPT.STUDY.ID
        },
        'links': {
            'investigator_id': CONCEPT.INVESTIGATOR.ID,
        },
        'properties': {
            'external_id': CONCEPT.STUDY.ID,
            'kf_id': CONCEPT.STUDY.TARGET_SERVICE_ID,
            'name': CONCEPT.STUDY.NAME,
            'short_name': CONCEPT.STUDY.SHORT_NAME,
            'version': CONCEPT.STUDY.VERSION,
            'data_access_authority': CONCEPT.STUDY.AUTHORITY,
            'release_status': CONCEPT.STUDY.RELEASE_STATUS,
            'attribution': CONCEPT.STUDY.ATTRIBUTION,
            'CATEGORY': CONCEPT.STUDY.CATEGORY
        }
    },
    'family': {
        'id': {
            'kf_id': CONCEPT.FAMILY.ID
        },
        'properties': {
            'external_id': CONCEPT.FAMILY.ID
        }
    },
    'participant': {
        'id': {
            'kf_id': CONCEPT.PARTICIPANT.ID
        },
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
        }
    },
    'diagnosis': {
        'id': {
            'kf_id': CONCEPT.DIAGNOSIS.ID
        },
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
        }
    },
    'phenotype': {
        'id': {
            'kf_id': CONCEPT.PHENOTYPE.ID
        },
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
        }
    },
    'outcome': {
        'id': {
            'kf_id': CONCEPT.OUTCOME.ID
        },
        'links': {
            'participant_id': CONCEPT.PARTICIPANT.ID
        },
        'properties': {
            "external_id": CONCEPT.OUTCOME.ID,
            "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
            "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
            "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
        }
    },
    'biospecimen': {
        'id': {
            'kf_id': CONCEPT.BIOSPECIMEN.ID
        },
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
        }
    },
    'genomic_file': {
        'id': {
            'kf_id': 'hey'
        },
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
        }
    },
    'read_group': {
        'id': {
            'kf_id': CONCEPT.READ_GROUP.ID
        },
        'links': {
            'genomic_file': CONCEPT.GENOMIC_FILE.ID
        },
        'properties': {
            "external_id": CONCEPT.READ_GROUP.ID,
            "flow_cell": CONCEPT.READ_GROUP.FLOW_CELL,
            "lane_number": CONCEPT.READ_GROUP.LANE_NUMBER,
            "quality_scale": CONCEPT.READ_GROUP.QUALITY_SCALE
        }
    },
    'sequencing_experiment': {
        'id': {
            'kf_id': CONCEPT.SEQUENCING.ID
        },
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
        }
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

# Transport configuration parameters
# Kids First entities to endpoint mapping
endpoints = {
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
