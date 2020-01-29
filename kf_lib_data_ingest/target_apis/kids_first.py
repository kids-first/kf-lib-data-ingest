"""
Configuration module specifying how a target model maps to the standard model.

This module is translated into an
etl.configuration.target_api_config.TargetAPIConfig object which is used by the
transform stage to populate the concept graph and by the load stage to populate
instances of target model concepts (i.e. participants, diagnoses, etc)
with data from the standard model before those instances are loaded into the
target service (i.e Kids First Dataservice)

See etl.configuration.target_api_config docstring for more details on
requirements for format and content.
"""
from kf_lib_data_ingest.common.concept_schema import CONCEPT, concept_set
from kf_lib_data_ingest.common.misc import str_to_obj

target_service_entity_id = "kf_id"


def indexd_hashes(dictstr):
    return {
        k.lower().replace("-", ""): v for k, v in str_to_obj(dictstr).items()
    }


target_concepts = {
    "investigator": {
        "standard_concept": CONCEPT.INVESTIGATOR,
        "properties": {
            target_service_entity_id: CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            "external_id": CONCEPT.INVESTIGATOR.UNIQUE_KEY,
            "name": CONCEPT.INVESTIGATOR.NAME,
            "institution": CONCEPT.INVESTIGATOR.INSTITUTION,
            "visible": CONCEPT.INVESTIGATOR.VISIBLE,
        },
        "endpoint": "/investigators",
    },
    "study": {
        "standard_concept": CONCEPT.STUDY,
        "links": [
            {
                "target_attribute": "investigator_id",
                "standard_concept": CONCEPT.INVESTIGATOR,
                "target_concept": "investigator",
            }
        ],
        "properties": {
            target_service_entity_id: CONCEPT.STUDY.TARGET_SERVICE_ID,
            "investigator_id": CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            "external_id": CONCEPT.STUDY.UNIQUE_KEY,
            "name": CONCEPT.STUDY.NAME,
            "short_name": CONCEPT.STUDY.SHORT_NAME,
            "version": CONCEPT.STUDY.VERSION,
            "data_access_authority": CONCEPT.STUDY.AUTHORITY,
            "release_status": CONCEPT.STUDY.RELEASE_STATUS,
            "attribution": CONCEPT.STUDY.ATTRIBUTION,
            "category": CONCEPT.STUDY.CATEGORY,
            "visible": CONCEPT.STUDY.VISIBLE,
        },
        "endpoint": "/studies",
    },
    "family": {
        "standard_concept": CONCEPT.FAMILY,
        "properties": {
            target_service_entity_id: CONCEPT.FAMILY.TARGET_SERVICE_ID,
            "external_id": CONCEPT.FAMILY.UNIQUE_KEY,
            "visible": CONCEPT.FAMILY.VISIBLE,
        },
        "endpoint": "/families",
    },
    "participant": {
        "standard_concept": CONCEPT.PARTICIPANT,
        "links": [
            {
                "target_attribute": "family_id",
                "standard_concept": CONCEPT.FAMILY,
                "target_concept": "family",
            },
            {
                "target_attribute": "study_id",
                "standard_concept": CONCEPT.STUDY,
                "target_concept": "study",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "study_id": CONCEPT.STUDY.TARGET_SERVICE_ID,
            "family_id": CONCEPT.FAMILY.TARGET_SERVICE_ID,
            "external_id": CONCEPT.PARTICIPANT.UNIQUE_KEY,
            "is_proband": CONCEPT.PARTICIPANT.IS_PROBAND,
            "ethnicity": CONCEPT.PARTICIPANT.ETHNICITY,
            "gender": CONCEPT.PARTICIPANT.GENDER,
            "race": CONCEPT.PARTICIPANT.RACE,
            "affected_status": CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY,
            "species": CONCEPT.PARTICIPANT.SPECIES,
            "visible": CONCEPT.PARTICIPANT.VISIBLE,
        },
        "endpoint": "/participants",
    },
    "family_relationship": {
        "standard_concept": CONCEPT.FAMILY_RELATIONSHIP,
        "links": [
            {
                "target_attribute": "participant1_id",
                "standard_concept": CONCEPT.FAMILY_RELATIONSHIP.PERSON1,
                "target_concept": "participant",
            },
            {
                "target_attribute": "participant2_id",
                "standard_concept": CONCEPT.FAMILY_RELATIONSHIP.PERSON2,
                "target_concept": "participant",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID,
            "participant1_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID,
            "participant2_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID,
            "external_id": CONCEPT.FAMILY_RELATIONSHIP.UNIQUE_KEY,
            "participant1_to_participant2_relation": CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2,
            "visible": CONCEPT.FAMILY_RELATIONSHIP.VISIBLE,
        },
        "endpoint": "/family-relationships",
    },
    "diagnosis": {
        "standard_concept": CONCEPT.DIAGNOSIS,
        "links": [
            {
                "target_attribute": "participant_id",
                "standard_concept": CONCEPT.PARTICIPANT,
                "target_concept": "participant",
            }
        ],
        "properties": {
            target_service_entity_id: CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "external_id": CONCEPT.DIAGNOSIS.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS,
            "source_text_diagnosis": CONCEPT.DIAGNOSIS.NAME,
            "source_text_tumor_location": CONCEPT.DIAGNOSIS.TUMOR_LOCATION,
            "mondo_id_diagnosis": CONCEPT.DIAGNOSIS.MONDO_ID,
            "icd_id_diagnosis": CONCEPT.DIAGNOSIS.ICD_ID,
            "uberon_id_tumor_location": CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID,
            "ncit_id_diagnosis": CONCEPT.DIAGNOSIS.NCIT_ID,
            "spatial_descriptor": CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR,
            "diagnosis_category": CONCEPT.DIAGNOSIS.CATEGORY,
            "visible": CONCEPT.DIAGNOSIS.VISIBLE,
        },
        "endpoint": "/diagnoses",
    },
    "phenotype": {
        "standard_concept": CONCEPT.PHENOTYPE,
        "links": [
            {
                "target_attribute": "participant_id",
                "standard_concept": CONCEPT.PARTICIPANT,
                "target_concept": "participant",
            }
        ],
        "properties": {
            target_service_entity_id: CONCEPT.PHENOTYPE.TARGET_SERVICE_ID,
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "external_id": CONCEPT.PHENOTYPE.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
            "source_text_phenotype": CONCEPT.PHENOTYPE.NAME,
            "hpo_id_phenotype": CONCEPT.PHENOTYPE.HPO_ID,
            "snomed_id_phenotype": CONCEPT.PHENOTYPE.SNOMED_ID,
            "observed": CONCEPT.PHENOTYPE.OBSERVED,
            "visible": CONCEPT.PHENOTYPE.VISIBLE,
        },
        "endpoint": "/phenotypes",
    },
    "outcome": {
        "standard_concept": CONCEPT.OUTCOME,
        "links": [
            {
                "target_attribute": "participant_id",
                "standard_concept": CONCEPT.PARTICIPANT,
                "target_concept": "participant",
            }
        ],
        "properties": {
            target_service_entity_id: CONCEPT.OUTCOME.TARGET_SERVICE_ID,
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "external_id": CONCEPT.OUTCOME.UNIQUE_KEY,
            "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
            "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
            "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
            "visible": CONCEPT.OUTCOME.VISIBLE,
        },
        "endpoint": "/outcomes",
    },
    "biospecimen": {
        "standard_concept": CONCEPT.BIOSPECIMEN,
        "links": [
            {
                "target_attribute": "sequencing_center_id",
                "standard_concept": CONCEPT.SEQUENCING.CENTER,
                "target_concept": "sequencing_center",
            },
            {
                "target_attribute": "participant_id",
                "standard_concept": CONCEPT.PARTICIPANT,
                "target_concept": "participant",
            },
        ],
        "properties": {
            "external_sample_id": CONCEPT.BIOSPECIMEN_GROUP.UNIQUE_KEY,
            "external_aliquot_id": CONCEPT.BIOSPECIMEN.ID,
            target_service_entity_id: CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "sequencing_center_id": CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID,
            "source_text_tissue_type": CONCEPT.BIOSPECIMEN.TISSUE_TYPE,
            "composition": CONCEPT.BIOSPECIMEN.COMPOSITION,
            "source_text_anatomical_site": CONCEPT.BIOSPECIMEN.ANATOMY_SITE,
            "age_at_event_days": CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS,
            "source_text_tumor_descriptor": CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR,
            "ncit_id_tissue_type": CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID,
            "ncit_id_anatomical_site": CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID,
            "spatial_descriptor": CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR,
            "shipment_origin": CONCEPT.BIOSPECIMEN.SHIPMENT_ORIGIN,
            "shipment_date": CONCEPT.BIOSPECIMEN.SHIPMENT_DATE,
            "analyte_type": CONCEPT.BIOSPECIMEN.ANALYTE,
            "concentration_mg_per_ml": CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML,
            "volume_ul": CONCEPT.BIOSPECIMEN.VOLUME_UL,
            "visible": CONCEPT.BIOSPECIMEN.VISIBLE,
        },
        "endpoint": "/biospecimens",
    },
    "genomic_file": {
        "standard_concept": CONCEPT.GENOMIC_FILE,
        "properties": {
            target_service_entity_id: CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.GENOMIC_FILE.UNIQUE_KEY,
            "file_name": CONCEPT.GENOMIC_FILE.FILE_NAME,
            "file_format": CONCEPT.GENOMIC_FILE.FILE_FORMAT,
            "data_type": CONCEPT.GENOMIC_FILE.DATA_TYPE,
            "availability": CONCEPT.GENOMIC_FILE.AVAILABILITY,
            "controlled_access": (
                CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS,
                str_to_obj,
            ),
            "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
            "hashes": (CONCEPT.GENOMIC_FILE.HASH_DICT, indexd_hashes),
            "size": (CONCEPT.GENOMIC_FILE.SIZE, int),
            "urls": (CONCEPT.GENOMIC_FILE.URL_LIST, str_to_obj),
            "acl": (CONCEPT.GENOMIC_FILE.ACL, str_to_obj),
            "reference_genome": CONCEPT.GENOMIC_FILE.REFERENCE_GENOME,
            "visible": CONCEPT.GENOMIC_FILE.VISIBLE,
        },
        "endpoint": "/genomic-files",
    },
    "read_group": {
        "standard_concept": CONCEPT.READ_GROUP,
        "properties": {
            target_service_entity_id: CONCEPT.READ_GROUP.TARGET_SERVICE_ID,
            "external_id": CONCEPT.READ_GROUP.UNIQUE_KEY,
            "flow_cell": CONCEPT.READ_GROUP.FLOW_CELL,
            "lane_number": CONCEPT.READ_GROUP.LANE_NUMBER,
            "quality_scale": CONCEPT.READ_GROUP.QUALITY_SCALE,
            "visible": CONCEPT.READ_GROUP.VISIBLE,
        },
        "endpoint": "/read-groups",
    },
    "sequencing_experiment": {
        "standard_concept": CONCEPT.SEQUENCING,
        "links": [
            {
                "target_attribute": "sequencing_center_id",
                "standard_concept": CONCEPT.SEQUENCING.CENTER,
                "target_concept": "sequencing_center",
            }
        ],
        "properties": {
            target_service_entity_id: CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
            "sequencing_center_id": CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID,
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
            "visible": CONCEPT.SEQUENCING.VISIBLE,
        },
        "endpoint": "/sequencing-experiments",
    },
    "sequencing_center": {
        "standard_concept": CONCEPT.SEQUENCING.CENTER,
        "properties": {
            target_service_entity_id: CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID,
            "external_id": CONCEPT.SEQUENCING.CENTER.UNIQUE_KEY,
            "target_attribute": CONCEPT.SEQUENCING.CENTER.NAME,
            "visible": CONCEPT.SEQUENCING.CENTER.VISIBLE,
        },
        "endpoint": "/sequencing-centers",
    },
    "biospecimen_genomic_file": {
        "standard_concept": CONCEPT.BIOSPECIMEN_GENOMIC_FILE,
        "links": [
            {
                "target_attribute": "biospecimen_id",
                "standard_concept": CONCEPT.BIOSPECIMEN,
                "target_concept": "biospecimen",
            },
            {
                "target_attribute": "genomic_file_id",
                "standard_concept": CONCEPT.GENOMIC_FILE,
                "target_concept": "genomic_file",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID,
            "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY,
            "visible": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE,
        },
        "endpoint": "/biospecimen-genomic-files",
    },
    "biospecimen_diagnosis": {
        "standard_concept": CONCEPT.BIOSPECIMEN_DIAGNOSIS,
        "links": [
            {
                "target_attribute": "biospecimen_id",
                "standard_concept": CONCEPT.BIOSPECIMEN,
                "target_concept": "biospecimen",
            },
            {
                "target_attribute": "diagnosis_id",
                "standard_concept": CONCEPT.DIAGNOSIS,
                "target_concept": "diagnosis",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID,
            "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "diagnosis_id": CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            "external_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.UNIQUE_KEY,
            "visible": CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE,
        },
        "endpoint": "/biospecimen-diagnoses",
    },
    "read_group_genomic_file": {
        "standard_concept": CONCEPT.READ_GROUP_GENOMIC_FILE,
        "links": [
            {
                "target_attribute": "read_group_id",
                "standard_concept": CONCEPT.READ_GROUP,
                "target_concept": "read_group",
            },
            {
                "target_attribute": "genomic_file_id",
                "standard_concept": CONCEPT.GENOMIC_FILE,
                "target_concept": "genomic_file",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.READ_GROUP_GENOMIC_FILE.TARGET_SERVICE_ID,
            "read_group_id": CONCEPT.READ_GROUP.TARGET_SERVICE_ID,
            "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.READ_GROUP_GENOMIC_FILE.UNIQUE_KEY,
            "visible": CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBLE,
        },
        "endpoint": "/read-group-genomic-files",
    },
    "sequencing_experiment_genomic_file": {
        "standard_concept": CONCEPT.SEQUENCING_GENOMIC_FILE,
        "links": [
            {
                "target_attribute": "sequencing_experiment_id",
                "standard_concept": CONCEPT.SEQUENCING,
                "target_concept": "sequencing_experiment",
            },
            {
                "target_attribute": "genomic_file_id",
                "standard_concept": CONCEPT.GENOMIC_FILE,
                "target_concept": "genomic_file",
            },
        ],
        "properties": {
            target_service_entity_id: CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID,
            "sequencing_experiment_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
            "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.SEQUENCING_GENOMIC_FILE.UNIQUE_KEY,
            "visible": CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE,
        },
        "endpoint": "/sequencing-experiment-genomic-files",
    },
}


def validate():
    """
    Custom validation specific to this target api config
    """
    for target_concept, schema in target_concepts.items():
        # Ensure every schema has a mapping for target service entity id and
        # linked target service entity ids
        std_concept = schema["standard_concept"]
        tgt_id_mapping = schema["properties"].get(target_service_entity_id)
        assert tgt_id_mapping == getattr(std_concept, "TARGET_SERVICE_ID"), (
            f"Properties schema for {target_concept} has an incorrect or null "
            f"mapping {target_service_entity_id}: {tgt_id_mapping}"
        )

        for link_dict in schema.get("links", []):
            link_std_concept = link_dict["standard_concept"]
            link_key = link_dict["target_attribute"]
            tgt_id_mapping = schema["properties"].get(link_key)
            assert tgt_id_mapping == getattr(
                link_std_concept, "TARGET_SERVICE_ID"
            ), (
                f'Links schema for "{target_concept}" has an '
                f"incorrect or null mapping, {link_key}: {tgt_id_mapping}"
            )


# TODO: This should be merged into the target API config!
def _create_unique_key_composition():
    """
    Build a dict which stores the attributes used to build a unique key
    for a particular concept. This key uniquely identifies concept instances of
    the same type.

    A key in the dict is a standard concept and a value in
    the dict is a set of concept attributes.

    - The ID attribute is usually populated with a unique identifier
    provided from the source data and so the default unique key for a concept
    is composed of just the ID attribute.

    - However, some concepts don't typically have a unique ID assigned in
    the source data. These concepts can be uniquely identified by a
    combination of one or more other concept attributes. Thus, the unique key
    would be composed of a set of attribute values joined together by some
    delimiter.

    - For example a phenotype doesn't typically have a unique identifier
    assigned to it in the source data. But a phenotype can be uniquely
    identified by a combination of the participant's id, phenotype name,
    observed status, and the age of the participant when the observation was
    recorded. The values for these attributes would form the unique key for a
    phenotype instance.
    """
    # Default unique keys
    identifiers = {}
    for concept in concept_set:
        identifiers[concept._CONCEPT_NAME] = {"required": [concept.ID]}

    # Compound unique keys
    identifiers[CONCEPT.BIOSPECIMEN._CONCEPT_NAME] = {
        "required": [CONCEPT.BIOSPECIMEN_GROUP.ID, CONCEPT.BIOSPECIMEN.ID]
    }
    identifiers[CONCEPT.INVESTIGATOR._CONCEPT_NAME] = {
        "required": [
            CONCEPT.INVESTIGATOR.NAME,
            CONCEPT.INVESTIGATOR.INSTITUTION,
        ]
    }
    identifiers[CONCEPT.DIAGNOSIS._CONCEPT_NAME] = {
        "required": [CONCEPT.PARTICIPANT.UNIQUE_KEY, CONCEPT.DIAGNOSIS.NAME],
        "optional": [CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS],
    }
    identifiers[CONCEPT.PHENOTYPE._CONCEPT_NAME] = {
        "required": [CONCEPT.PARTICIPANT.UNIQUE_KEY, CONCEPT.PHENOTYPE.NAME],
        "optional": [
            CONCEPT.PHENOTYPE.OBSERVED,
            CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
        ],
    }
    identifiers[CONCEPT.OUTCOME._CONCEPT_NAME] = {
        "required": [
            CONCEPT.PARTICIPANT.UNIQUE_KEY,
            CONCEPT.OUTCOME.VITAL_STATUS,
        ],
        "optional": [CONCEPT.OUTCOME.EVENT_AGE_DAYS],
    }
    identifiers[CONCEPT.BIOSPECIMEN_GENOMIC_FILE._CONCEPT_NAME] = {
        "required": [
            CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
            CONCEPT.GENOMIC_FILE.UNIQUE_KEY,
        ]
    }
    identifiers[CONCEPT.BIOSPECIMEN_DIAGNOSIS._CONCEPT_NAME] = {
        "required": [
            CONCEPT.BIOSPECIMEN.UNIQUE_KEY,
            CONCEPT.DIAGNOSIS.UNIQUE_KEY,
        ]
    }
    identifiers[CONCEPT.READ_GROUP_GENOMIC_FILE._CONCEPT_NAME] = {
        "required": [
            CONCEPT.READ_GROUP.UNIQUE_KEY,
            CONCEPT.GENOMIC_FILE.UNIQUE_KEY,
        ]
    }
    identifiers[CONCEPT.SEQUENCING_GENOMIC_FILE._CONCEPT_NAME] = {
        "required": [
            CONCEPT.SEQUENCING.UNIQUE_KEY,
            CONCEPT.GENOMIC_FILE.UNIQUE_KEY,
        ]
    }
    identifiers[CONCEPT.FAMILY_RELATIONSHIP._CONCEPT_NAME] = {
        "required": [
            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.UNIQUE_KEY,
            CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2,
            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.UNIQUE_KEY,
        ]
    }
    return identifiers


unique_key_composition = _create_unique_key_composition()
