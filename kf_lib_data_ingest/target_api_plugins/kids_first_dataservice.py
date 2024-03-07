"""
This module is translated into a etl.configuration.target_api_config.TargetAPIConfig
object which is used by the load stage to populate instances of target model
entities (i.e. participants, diagnoses, etc) with data from the extracted
concepts before those instances are loaded into the target service (i.e Kids
First Dataservice)

See etl.configuration.target_api_config docstring for more details on the
requirements for format and content.
"""

import logging
from threading import Lock

from d3b_utils.requests_retry import Session
from kf_utils.dataservice.scrape import yield_entities, yield_kfids
from pandas import DataFrame
from requests import RequestException

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.family_relationships import (
    convert_relationships_to_p1p2,
)
from kf_lib_data_ingest.common.misc import (
    flexible_age,
    str_to_obj,
    upper_camel_case,
)
from kf_lib_data_ingest.network.utils import get_open_api_v2_schema

logger = logging.getLogger(__name__)

LOADER_VERSION = 2
DELIMITER = "-"


def drop_none(body):
    return {k: v for k, v in body.items() if v is not None}


def not_none(val):
    if val is None:
        raise ValueError("Missing required value")
    return val


def external_id(cls_list, record, get_target_id_from_record):
    try:
        out = []
        for cls in cls_list:
            cls_key_dict = cls.get_key_components(
                record, get_target_id_from_record
            ).copy()
            for key in ["study_id", "sequencing_center_id"]:
                cls_key_dict.pop(key, None)
            out.extend([str(v) for v in cls_key_dict.values()])
    except (KeyError, ValueError):
        return None
    else:
        return DELIMITER.join(out) or None


class Investigator:
    class_name = "investigator"
    api_path = "investigators"
    target_id_concept = CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID
    service_id_fields = {"kf_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "external_id": record.get(CONCEPT.INVESTIGATOR.ID),
            "name": not_none(record[CONCEPT.INVESTIGATOR.NAME]),
            "institution": not_none(record[CONCEPT.INVESTIGATOR.INSTITUTION]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.INVESTIGATOR.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.INVESTIGATOR.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.INVESTIGATOR.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Study:
    class_name = "study"
    api_path = "studies"
    target_id_concept = CONCEPT.STUDY.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "investigator_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        kfid = record.get(cls.target_id_concept) or record.get(
            CONCEPT.PROJECT.ID
        )
        au = record.get(CONCEPT.STUDY.AUTHORITY)
        id = record.get(CONCEPT.STUDY.ID)
        assert (au and id) or kfid
        return {"kf_id": kfid, "data_access_authority": au, "external_id": id}

    @classmethod
    def query_target_ids(cls, host, key_components):
        kfid = key_components.pop("kf_id", None)
        if kfid:
            return [kfid]
        else:
            return list(
                yield_kfids(host, cls.api_path, drop_none(key_components))
            )

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "investigator_id": get_target_id_from_record(Investigator, record),
            "name": record.get(CONCEPT.STUDY.NAME),
            "short_name": record.get(CONCEPT.STUDY.SHORT_NAME),
            "version": record.get(CONCEPT.STUDY.VERSION),
            "release_status": record.get(CONCEPT.STUDY.RELEASE_STATUS),
            "attribution": record.get(CONCEPT.STUDY.ATTRIBUTION),
            "category": record.get(CONCEPT.STUDY.CATEGORY),
            "visible": record.get(CONCEPT.STUDY.VISIBLE),
            "visibility_comment": record.get(CONCEPT.STUDY.VISIBILITY_COMMENT),
            "visibility_reason": record.get(CONCEPT.STUDY.VISIBILTIY_REASON),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Family:
    class_name = "family"
    api_path = "families"
    target_id_concept = CONCEPT.FAMILY.TARGET_SERVICE_ID
    service_id_fields = {"kf_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "study_id": get_target_id_from_record(Study, record),
            "external_id": not_none(record[CONCEPT.FAMILY.ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.FAMILY.VISIBLE),
            "visibility_comment": record.get(CONCEPT.FAMILY.VISIBILITY_COMMENT),
            "visibility_reason": record.get(CONCEPT.FAMILY.VISIBILTIY_REASON),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Participant:
    class_name = "participant"
    api_path = "participants"
    target_id_concept = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "family_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "study_id": get_target_id_from_record(Study, record),
            "external_id": not_none(record[CONCEPT.PARTICIPANT.ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        # family foreign key is optional
        try:
            family_id = get_target_id_from_record(Family, record)
        except Exception:
            family_id = None

        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "family_id": family_id,
            "is_proband": record.get(CONCEPT.PARTICIPANT.IS_PROBAND),
            "ethnicity": record.get(CONCEPT.PARTICIPANT.ETHNICITY),
            "gender": record.get(CONCEPT.PARTICIPANT.GENDER),
            "race": record.get(CONCEPT.PARTICIPANT.RACE),
            "affected_status": record.get(
                CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY
            ),
            "species": record.get(CONCEPT.PARTICIPANT.SPECIES),
            "visible": record.get(CONCEPT.PARTICIPANT.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.PARTICIPANT.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.PARTICIPANT.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Diagnosis:
    class_name = "diagnosis"
    api_path = "diagnoses"
    target_id_concept = CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "participant_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "participant_id": not_none(
                get_target_id_from_record(Participant, record)
            ),
            "source_text_diagnosis": not_none(record[CONCEPT.DIAGNOSIS.NAME]),
            "age_at_event_days": flexible_age(
                record,
                CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS,
                CONCEPT.DIAGNOSIS.EVENT_AGE,
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "source_text_tumor_location": record.get(
                CONCEPT.DIAGNOSIS.TUMOR_LOCATION
            ),
            "mondo_id_diagnosis": record.get(CONCEPT.DIAGNOSIS.MONDO_ID),
            "icd_id_diagnosis": record.get(CONCEPT.DIAGNOSIS.ICD_ID),
            "uberon_id_tumor_location": record.get(
                CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID
            ),
            "ncit_id_diagnosis": record.get(CONCEPT.DIAGNOSIS.NCIT_ID),
            "spatial_descriptor": record.get(
                CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR
            ),
            "diagnosis_category": record.get(CONCEPT.DIAGNOSIS.CATEGORY),
            "visible": record.get(CONCEPT.DIAGNOSIS.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.DIAGNOSIS.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.DIAGNOSIS.VISIBILTIY_REASON
            ),
            "external_id": record.get(CONCEPT.DIAGNOSIS.ID),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Phenotype:
    class_name = "phenotype"
    api_path = "phenotypes"
    target_id_concept = CONCEPT.PHENOTYPE.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "participant_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "participant_id": not_none(
                get_target_id_from_record(Participant, record)
            ),
            "source_text_phenotype": not_none(record[CONCEPT.PHENOTYPE.NAME]),
            "observed": not_none(record[CONCEPT.PHENOTYPE.OBSERVED]),
            "age_at_event_days": flexible_age(
                record,
                CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
                CONCEPT.PHENOTYPE.EVENT_AGE,
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "hpo_id_phenotype": record.get(CONCEPT.PHENOTYPE.HPO_ID),
            "snomed_id_phenotype": record.get(CONCEPT.PHENOTYPE.SNOMED_ID),
            "visible": record.get(CONCEPT.PHENOTYPE.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.PHENOTYPE.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.PHENOTYPE.VISIBILTIY_REASON
            ),
            "external_id": record.get(CONCEPT.PHENOTYPE.ID),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Outcome:
    class_name = "outcome"
    api_path = "outcomes"
    target_id_concept = CONCEPT.OUTCOME.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "participant_id"}

    @classmethod
    def transform_records_list(cls, records_list):
        # We no longer want multiple participant outcome entries, and we now
        # always patch the latest one with whatever is in the record for
        # existing dataservice compatibility, so sort the records so that the
        # latest ones appear first (the others will then get skipped).
        return sorted(
            records_list,
            reverse=True,
            key=lambda r: (
                flexible_age(
                    r,
                    CONCEPT.OUTCOME.EVENT_AGE_DAYS,
                    CONCEPT.OUTCOME.EVENT_AGE,
                )
                or 0
            ),
        )

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        # Skip anything without a status, but don't consider it a key field.
        not_none(record[CONCEPT.OUTCOME.VITAL_STATUS])
        return {
            "participant_id": not_none(
                get_target_id_from_record(Participant, record)
            )
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        # We no longer want multiple participant outcome entries.
        # Patch whatever is latest for compatibility with existing dataservice
        # entries.
        pes = sorted(
            yield_entities(host, cls.api_path, key_components),
            key=lambda e: e.get("age_at_event_days", 0),
        )
        if pes:
            return [pes[-1]["kf_id"]]
        else:
            return []

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "disease_related": record.get(CONCEPT.OUTCOME.DISEASE_RELATED),
            "visible": record.get(CONCEPT.OUTCOME.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.OUTCOME.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(CONCEPT.OUTCOME.VISIBILTIY_REASON),
            "age_at_event_days": flexible_age(
                record,
                CONCEPT.OUTCOME.EVENT_AGE_DAYS,
                CONCEPT.OUTCOME.EVENT_AGE,
            ),
            "vital_status": not_none(record[CONCEPT.OUTCOME.VITAL_STATUS]),
            "external_id": record.get(CONCEPT.OUTCOME.ID),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class Biospecimen:
    class_name = "biospecimen"
    api_path = "biospecimens"
    target_id_concept = CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "participant_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "study_id": get_target_id_from_record(Study, record),
            "external_aliquot_id": not_none(record[CONCEPT.BIOSPECIMEN.ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "sequencing_center_id": record.get(
                CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
            ),
            "participant_id": not_none(
                get_target_id_from_record(Participant, record)
            ),
            "external_sample_id": (
                record.get(CONCEPT.BIOSPECIMEN_GROUP.ID)
                or not_none(record[CONCEPT.BIOSPECIMEN.ID])
            ),
            "source_text_tissue_type": record.get(
                CONCEPT.BIOSPECIMEN.TISSUE_TYPE
            ),
            "composition": record.get(CONCEPT.BIOSPECIMEN.COMPOSITION),
            "source_text_anatomical_site": record.get(
                CONCEPT.BIOSPECIMEN.ANATOMY_SITE
            ),
            "age_at_event_days": flexible_age(
                record,
                CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS,
                CONCEPT.BIOSPECIMEN.EVENT_AGE,
            ),
            "source_text_tumor_descriptor": record.get(
                CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR
            ),
            "ncit_id_tissue_type": record.get(
                CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID
            ),
            "ncit_id_anatomical_site": record.get(
                CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID
            ),
            "uberon_id_anatomical_site": record.get(
                CONCEPT.BIOSPECIMEN.UBERON_ANATOMY_SITE_ID
            ),
            "spatial_descriptor": record.get(
                CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR
            ),
            "shipment_origin": record.get(CONCEPT.BIOSPECIMEN.SHIPMENT_ORIGIN),
            "shipment_date": record.get(CONCEPT.BIOSPECIMEN.SHIPMENT_DATE),
            "analyte_type": record.get(CONCEPT.BIOSPECIMEN.ANALYTE),
            "concentration_mg_per_ml": record.get(
                CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML
            ),
            "volume_ul": record.get(CONCEPT.BIOSPECIMEN.VOLUME_UL),
            "visible": record.get(CONCEPT.BIOSPECIMEN.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.BIOSPECIMEN.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.BIOSPECIMEN.VISIBILTIY_REASON
            ),
            "method_of_sample_procurement": record.get(
                CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT
            ),
            "dbgap_consent_code": record.get(
                CONCEPT.BIOSPECIMEN.DBGAP_STYLE_CONSENT_CODE
            ),
            "consent_type": record.get(CONCEPT.BIOSPECIMEN.CONSENT_SHORT_NAME),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class GenomicFile:
    class_name = "genomic_file"
    api_path = "genomic-files"
    target_id_concept = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID
    service_id_fields = {"kf_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        # FIXME: Temporary until KFDRC file hashes are reliably stable
        return {
            "study_id": get_target_id_from_record(Study, record),
            "external_id": not_none(record[CONCEPT.GENOMIC_FILE.ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        def size(record):
            try:
                return int(record.get(CONCEPT.GENOMIC_FILE.SIZE))
            except Exception:
                return None

        def hashes(record):
            return {
                k.lower().replace("-", ""): v
                for k, v in str_to_obj(
                    record.get(CONCEPT.GENOMIC_FILE.HASH_DICT, {})
                ).items()
            }

        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "file_name": record.get(CONCEPT.GENOMIC_FILE.FILE_NAME),
            "file_format": record.get(CONCEPT.GENOMIC_FILE.FILE_FORMAT),
            "data_type": record.get(CONCEPT.GENOMIC_FILE.DATA_TYPE),
            "availability": record.get(CONCEPT.GENOMIC_FILE.AVAILABILITY),
            "controlled_access": str_to_obj(
                record.get(CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS)
            ),
            "is_harmonized": record.get(CONCEPT.GENOMIC_FILE.HARMONIZED),
            "hashes": hashes(record),
            "size": size(record),
            "urls": str_to_obj(record.get(CONCEPT.GENOMIC_FILE.URL_LIST)),
            "acl": [],
            "authz": str_to_obj(record.get(CONCEPT.GENOMIC_FILE.ACL)),
            "reference_genome": record.get(
                CONCEPT.GENOMIC_FILE.REFERENCE_GENOME
            ),
            "visible": record.get(CONCEPT.GENOMIC_FILE.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.GENOMIC_FILE.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.GENOMIC_FILE.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class ReadGroup:
    class_name = "read_group"
    api_path = "read-groups"
    target_id_concept = CONCEPT.READ_GROUP.TARGET_SERVICE_ID
    service_id_fields = {"kf_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "study_id": get_target_id_from_record(Study, record),
            "external_id": record.get(CONCEPT.READ_GROUP.ID),
            "flow_cell": not_none(record[CONCEPT.READ_GROUP.FLOW_CELL]),
            "lane_number": not_none(record[CONCEPT.READ_GROUP.LANE_NUMBER]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "quality_scale": record.get(CONCEPT.READ_GROUP.QUALITY_SCALE),
            "visible": record.get(CONCEPT.READ_GROUP.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.READ_GROUP.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.READ_GROUP.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class SequencingExperiment:
    class_name = "sequencing_experiment"
    api_path = "sequencing-experiments"
    target_id_concept = CONCEPT.SEQUENCING.TARGET_SERVICE_ID
    service_id_fields = {"kf_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "study_id": get_target_id_from_record(Study, record),
            "sequencing_center_id": not_none(
                record[CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID]
            ),
            "external_id": not_none(record[CONCEPT.SEQUENCING.ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "library_name": record.get(CONCEPT.SEQUENCING.LIBRARY_NAME),
            "kf_id": get_target_id_from_record(cls, record),
            "experiment_date": record.get(CONCEPT.SEQUENCING.DATE),
            "experiment_strategy": record.get(CONCEPT.SEQUENCING.STRATEGY),
            "library_strand": record.get(CONCEPT.SEQUENCING.LIBRARY_STRAND),
            "library_prep": record.get(CONCEPT.SEQUENCING.LIBRARY_PREP),
            "library_selection": record.get(
                CONCEPT.SEQUENCING.LIBRARY_SELECTION
            ),
            "is_paired_end": record.get(CONCEPT.SEQUENCING.PAIRED_END),
            "platform": record.get(CONCEPT.SEQUENCING.PLATFORM),
            "instrument_model": record.get(CONCEPT.SEQUENCING.INSTRUMENT),
            "max_insert_size": record.get(CONCEPT.SEQUENCING.MAX_INSERT_SIZE),
            "mean_insert_size": record.get(CONCEPT.SEQUENCING.MEAN_INSERT_SIZE),
            "mean_depth": record.get(CONCEPT.SEQUENCING.MEAN_DEPTH),
            "total_reads": record.get(CONCEPT.SEQUENCING.TOTAL_READS),
            "mean_read_length": record.get(CONCEPT.SEQUENCING.MEAN_READ_LENGTH),
            "visible": record.get(CONCEPT.SEQUENCING.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.SEQUENCING.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.SEQUENCING.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class FamilyRelationship:
    class_name = "family_relationship"
    api_path = "family-relationships"
    target_id_concept = CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "participant1_id", "participant2_id"}

    @classmethod
    def transform_records_list(cls, records_list):
        df = convert_relationships_to_p1p2(
            DataFrame(records_list), infer_genders=True, bidirect=True
        )
        return df.to_dict("records")

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "participant1_id": not_none(
                get_target_id_from_record(
                    Participant,
                    {
                        CONCEPT.PROJECT.ID: record[CONCEPT.PROJECT.ID],
                        CONCEPT.PARTICIPANT.ID: record[
                            CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID
                        ],
                    },
                )
            ),
            "participant2_id": not_none(
                get_target_id_from_record(
                    Participant,
                    {
                        CONCEPT.PROJECT.ID: record[CONCEPT.PROJECT.ID],
                        CONCEPT.PARTICIPANT.ID: record[
                            CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID
                        ],
                    },
                )
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "participant1_to_participant2_relation": record[
                CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
            ],
            "visible": record.get(CONCEPT.FAMILY_RELATIONSHIP.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.FAMILY_RELATIONSHIP.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.FAMILY_RELATIONSHIP.VISIBILTIY_REASON
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class BiospecimenGenomicFile:
    class_name = "biospecimen_genomic_file"
    api_path = "biospecimen-genomic-files"
    target_id_concept = CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "biospecimen_id", "genomic_file_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "biospecimen_id": not_none(
                get_target_id_from_record(Biospecimen, record)
            ),
            "genomic_file_id": not_none(
                get_target_id_from_record(GenomicFile, record)
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBILTIY_REASON
            ),
            "external_id": external_id(
                [Biospecimen, GenomicFile], record, get_target_id_from_record
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class BiospecimenDiagnosis:
    class_name = "biospecimen_diagnosis"
    api_path = "biospecimen-diagnoses"
    target_id_concept = CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "biospecimen_id", "diagnosis_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "biospecimen_id": not_none(
                get_target_id_from_record(Biospecimen, record)
            ),
            "diagnosis_id": not_none(
                get_target_id_from_record(Diagnosis, record)
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBILTIY_REASON
            ),
            "external_id": external_id(
                [Biospecimen, Diagnosis], record, get_target_id_from_record
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class ReadGroupGenomicFile:
    class_name = "read_group_genomic_file"
    api_path = "read-group-genomic-files"
    target_id_concept = CONCEPT.READ_GROUP_GENOMIC_FILE.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "read_group_id", "genomic_file_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "read_group_id": not_none(
                get_target_id_from_record(ReadGroup, record)
            ),
            "genomic_file_id": not_none(
                get_target_id_from_record(GenomicFile, record)
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBILTIY_REASON
            ),
            "external_id": external_id(
                [ReadGroup, GenomicFile], record, get_target_id_from_record
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


class SequencingExperimentGenomicFile:
    class_name = "sequencing_experiment_genomic_file"
    api_path = "sequencing-experiment-genomic-files"
    target_id_concept = CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID
    service_id_fields = {"kf_id", "sequencing_experiment_id", "genomic_file_id"}

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "sequencing_experiment_id": not_none(
                get_target_id_from_record(SequencingExperiment, record)
            ),
            "genomic_file_id": not_none(
                get_target_id_from_record(GenomicFile, record)
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_kfids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        secondary_components = {
            "kf_id": get_target_id_from_record(cls, record),
            "visible": record.get(CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE),
            "visibility_comment": record.get(
                CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILITY_COMMENT
            ),
            "visibility_reason": record.get(
                CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBILTIY_REASON
            ),
            "external_id": external_id(
                [SequencingExperiment, GenomicFile],
                record,
                get_target_id_from_record,
            ),
        }
        return {
            **cls.get_key_components(record, get_target_id_from_record),
            **secondary_components,
        }

    @classmethod
    def submit(cls, host, body):
        return submit(host, cls, body)


def _PATCH(host, api_path, kf_id, body):
    return Session().patch(
        url="/".join([v.strip("/") for v in [host, api_path, kf_id]]),
        json=body,
    )


def _POST(host, api_path, body):
    return Session().post(
        url="/".join([v.strip("/") for v in [host, api_path]]), json=body
    )


def _GET(host, api_path, body):
    return Session().get(
        url="/".join([v.strip("/") for v in [host, api_path]]),
        params={k: v for k, v in body.items() if v is not None},
    )


# in the order to be loaded
all_targets = [
    Investigator,
    Study,
    Family,
    Participant,
    FamilyRelationship,
    Diagnosis,
    Phenotype,
    Outcome,
    Biospecimen,
    GenomicFile,
    ReadGroup,
    SequencingExperiment,
    BiospecimenGenomicFile,
    BiospecimenDiagnosis,
    ReadGroupGenomicFile,
    SequencingExperimentGenomicFile,
]

swagger_cache = {}
json_type_casts = {
    "string": str,
    "integer": int,
    "number": float,
    "object": str_to_obj,
}
swag = Lock()


# The dataservice lies to us sometimes about how big ints can be
# so we're going to keep track ourselves
seen_overmax_int = {}


def coerce_types(host, entity_class, body):
    with swag:
        if not swagger_cache:
            swagger = get_open_api_v2_schema(host, logger=logger)
            defs = swagger["definitions"]
            for c in all_targets:
                n = c.class_name
                uccn = upper_camel_case(n)
                if uccn in defs:
                    swagger_cache[n] = defs[uccn]

    properties = swagger_cache[entity_class.class_name]["properties"]

    ret = {}
    for k, v in body.items():
        if (k not in properties) or properties[k].get("readOnly"):
            # e.g. modified_at/created_at
            continue
        elif (v is None) or (v == ""):
            if properties[k]["type"] == "string":
                if "date" in k:  # TODO: FIX THE DATASERVICE?
                    ret[k] = None
                elif k not in entity_class.service_id_fields:
                    ret[k] = constants.COMMON.NOT_REPORTED
                else:
                    ret[k] = None
            elif properties[k].get("x-nullable"):
                ret[k] = None
            else:
                # The field is required but we have no value.
                # Don't include it so that the server applies default behavior.
                continue
        else:
            v = json_type_casts.get(properties[k]["type"], lambda x: x)(v)
            if properties[k]["type"] == "integer":
                if entity_class.class_name not in seen_overmax_int:
                    seen_overmax_int[entity_class.class_name] = set()
                if k not in seen_overmax_int[entity_class.class_name]:
                    try:
                        max_value = (
                            2 ** (int(properties[k]["format"][-2:]) - 1)
                        ) - 1
                        if v > max_value:
                            logger.info(
                                f"The server indicates that {entity_class.class_name}"
                                f" field {k} may have maximum value {max_value}, but"
                                f" {v} was given. If the next request fails,"
                                " this might be why."
                            )
                            seen_overmax_int[entity_class.class_name].add(k)
                    except Exception:
                        pass
            ret[k] = v

    return ret


def submit(host, entity_class, body):
    """
    Negotiate submitting the data for an entity to the target service.

    :param host: host url
    :type host: str
    :param entity_class: which entity class is being sent
    :type entity_class: class
    :param body: map between entity keys and values
    :type body: dict
    :return: The target entity ID that the service says was created or updated
    :rtype: str
    :raise: RequestException on error
    """
    resp = None
    kf_id = body.get("kf_id")
    api_path = entity_class.api_path

    body = coerce_types(host, entity_class, body)

    if kf_id:
        resp = _PATCH(host, api_path, kf_id, body)
        if resp.status_code == 404:
            resp = None
    else:
        body.pop("kf_id", None)

    if not resp:
        resp = _POST(host, api_path, body)

    if resp.status_code in {200, 201}:
        try:
            return resp.json()["results"]["kf_id"]
        except Exception as e:
            raise RequestException(
                f"Got status 200/201 from /{api_path} but response was not "
                f"as expected:\nSent:\n{body}\nGot:\n{resp.text}"
            ) from e
    elif (resp.status_code == 400) and (
        "already exists" in resp.json()["_status"]["message"]
    ):
        # Our dataservice returns 400 if a relationship already exists
        # even though that's a silly thing to do.
        # See https://github.com/kids-first/kf-api-dataservice/issues/419
        extid = body.pop("external_id", None)
        resp = _GET(host, api_path, body)
        result = resp.json()["results"][0]
        if extid != result["external_id"]:
            resp = _PATCH(
                host, api_path, result["kf_id"], {"external_id": extid}
            )
            result = resp.json()["results"]
        return result["kf_id"]
    else:
        raise RequestException(
            f"Sent to /{api_path}:\n{body}\nGot:\n{resp.text}"
        )
