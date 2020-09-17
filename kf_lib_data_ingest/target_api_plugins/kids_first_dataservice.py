"""
This module is translated into a etl.configuration.target_api_config.TargetAPIConfig
object which is used by the load stage to populate instances of target model
entities (i.e. participants, diagnoses, etc) with data from the extracted
concepts before those instances are loaded into the target service (i.e Kids
First Dataservice)

See etl.configuration.target_api_config docstring for more details on the
requirements for format and content.
"""
from d3b_utils.requests_retry import Session
from requests import RequestException

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.misc import str_to_obj, upper_camel_case
from kf_lib_data_ingest.network.utils import get_open_api_v2_schema


def indexd_hashes(dictstr):
    return {
        k.lower().replace("-", ""): v for k, v in str_to_obj(dictstr).items()
    }


def join(*args):
    """
    Joins args using periods.
    This is used when making compound unique keys from record data.

    :return: `".".join([str(a) for a in args])`
    :rtype: str
    """
    return "|".join([str(a) for a in args])


class Investigator:
    class_name = "investigator"
    api_path = "/investigators"
    target_id_concept = CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID
    id_fields = {"kf_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.INVESTIGATOR.NAME]
        assert None is not record[CONCEPT.INVESTIGATOR.INSTITUTION]
        return record.get(CONCEPT.INVESTIGATOR.UNIQUE_KEY) or join(
            record[CONCEPT.INVESTIGATOR.NAME],
            record[CONCEPT.INVESTIGATOR.INSTITUTION],
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Investigator, record),
            "external_id": key,
            "name": record.get(CONCEPT.INVESTIGATOR.NAME),
            "institution": record.get(CONCEPT.INVESTIGATOR.INSTITUTION),
            "visible": record.get(CONCEPT.INVESTIGATOR.VISIBLE),
        }


class Study:
    class_name = "study"
    api_path = "/studies"
    target_id_concept = CONCEPT.STUDY.TARGET_SERVICE_ID
    id_fields = {"kf_id", "investigator_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.STUDY.ID]
        return record.get(CONCEPT.STUDY.UNIQUE_KEY) or join(
            record[CONCEPT.STUDY.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Study, record),
            "external_id": key,
            "investigator_id": get_target_id_from_record(Investigator, record),
            "name": record.get(CONCEPT.STUDY.NAME),
            "short_name": record.get(CONCEPT.STUDY.SHORT_NAME),
            "version": record.get(CONCEPT.STUDY.VERSION),
            "data_access_authority": record.get(CONCEPT.STUDY.AUTHORITY),
            "release_status": record.get(CONCEPT.STUDY.RELEASE_STATUS),
            "attribution": record.get(CONCEPT.STUDY.ATTRIBUTION),
            "category": record.get(CONCEPT.STUDY.CATEGORY),
            "visible": record.get(CONCEPT.STUDY.VISIBLE),
        }


class Family:
    class_name = "family"
    api_path = "/families"
    target_id_concept = CONCEPT.FAMILY.TARGET_SERVICE_ID
    id_fields = {"kf_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.FAMILY.ID]
        return record.get(CONCEPT.FAMILY.UNIQUE_KEY) or join(
            record[CONCEPT.FAMILY.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Family, record),
            "external_id": key,
            "visible": record.get(CONCEPT.FAMILY.VISIBLE),
        }


class Participant:
    class_name = "participant"
    api_path = "/participants"
    target_id_concept = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
    id_fields = {"kf_id", "family_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.PARTICIPANT.ID]
        return record.get(CONCEPT.PARTICIPANT.UNIQUE_KEY) or join(
            record[CONCEPT.PARTICIPANT.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Participant, record),
            "study_id": record[CONCEPT.STUDY.ID],
            "family_id": get_target_id_from_record(Family, record),
            "external_id": key,
            "is_proband": record.get(CONCEPT.PARTICIPANT.IS_PROBAND),
            "ethnicity": record.get(CONCEPT.PARTICIPANT.ETHNICITY),
            "gender": record.get(CONCEPT.PARTICIPANT.GENDER),
            "race": record.get(CONCEPT.PARTICIPANT.RACE),
            "affected_status": record.get(
                CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY
            ),
            "species": record.get(CONCEPT.PARTICIPANT.SPECIES),
            "visible": record.get(CONCEPT.PARTICIPANT.VISIBLE),
        }


class Diagnosis:
    class_name = "diagnosis"
    api_path = "/diagnoses"
    target_id_concept = CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID
    id_fields = {"kf_id", "participant_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.PARTICIPANT.ID]
        assert None is not record[CONCEPT.DIAGNOSIS.NAME]
        return record.get(CONCEPT.DIAGNOSIS.UNIQUE_KEY) or join(
            record[CONCEPT.PARTICIPANT.ID],
            record[CONCEPT.DIAGNOSIS.NAME],
            record.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Diagnosis, record),
            "participant_id": get_target_id_from_record(Participant, record),
            "external_id": key,
            "age_at_event_days": record.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS),
            "source_text_diagnosis": record.get(CONCEPT.DIAGNOSIS.NAME),
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
        }


class Phenotype:
    class_name = "phenotype"
    api_path = "/phenotypes"
    target_id_concept = CONCEPT.PHENOTYPE.TARGET_SERVICE_ID
    id_fields = {"kf_id", "participant_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.PARTICIPANT.ID]
        assert None is not record[CONCEPT.PHENOTYPE.NAME]
        assert record[CONCEPT.PHENOTYPE.OBSERVED] in {
            constants.PHENOTYPE.OBSERVED.NO,
            constants.PHENOTYPE.OBSERVED.YES,
        }
        return record.get(CONCEPT.PHENOTYPE.UNIQUE_KEY) or join(
            record[CONCEPT.PARTICIPANT.ID],
            record[CONCEPT.PHENOTYPE.NAME],
            record[
                CONCEPT.PHENOTYPE.OBSERVED
            ],  # TODO: WE SHOULD REMOVE OBSERVED
            record.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Phenotype, record),
            "participant_id": get_target_id_from_record(Participant, record),
            "external_id": key,
            "age_at_event_days": record.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS),
            "source_text_phenotype": record.get(CONCEPT.PHENOTYPE.NAME),
            "hpo_id_phenotype": record.get(CONCEPT.PHENOTYPE.HPO_ID),
            "snomed_id_phenotype": record.get(CONCEPT.PHENOTYPE.SNOMED_ID),
            "observed": record.get(CONCEPT.PHENOTYPE.OBSERVED),
            "visible": record.get(CONCEPT.PHENOTYPE.VISIBLE),
        }


class Outcome:
    class_name = "outcome"
    api_path = "/outcomes"
    target_id_concept = CONCEPT.OUTCOME.TARGET_SERVICE_ID
    id_fields = {"kf_id", "participant_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.PARTICIPANT.ID]
        assert None is not record[CONCEPT.OUTCOME.VITAL_STATUS]
        return record.get(CONCEPT.OUTCOME.UNIQUE_KEY) or join(
            record[CONCEPT.PARTICIPANT.ID],
            record[CONCEPT.OUTCOME.VITAL_STATUS],
            record.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Outcome, record),
            "participant_id": get_target_id_from_record(Participant, record),
            "external_id": key,
            "age_at_event_days": record.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS),
            "vital_status": record.get(CONCEPT.OUTCOME.VITAL_STATUS),
            "disease_related": record.get(CONCEPT.OUTCOME.DISEASE_RELATED),
            "visible": record.get(CONCEPT.OUTCOME.VISIBLE),
        }


class Biospecimen:
    class_name = "biospecimen"
    api_path = "/biospecimens"
    target_id_concept = CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID
    id_fields = {"kf_id", "participant_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.BIOSPECIMEN_GROUP.ID]
        assert None is not record[CONCEPT.BIOSPECIMEN.ID]
        assert record[CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID]
        return record.get(CONCEPT.BIOSPECIMEN.UNIQUE_KEY) or join(
            record[CONCEPT.BIOSPECIMEN_GROUP.ID], record[CONCEPT.BIOSPECIMEN.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(Biospecimen, record),
            "participant_id": get_target_id_from_record(Participant, record),
            "external_sample_id": record.get(CONCEPT.BIOSPECIMEN_GROUP.ID),
            "external_aliquot_id": record.get(CONCEPT.BIOSPECIMEN.ID),
            "sequencing_center_id": record.get(
                CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
            ),
            "source_text_tissue_type": record.get(
                CONCEPT.BIOSPECIMEN.TISSUE_TYPE
            ),
            "composition": record.get(CONCEPT.BIOSPECIMEN.COMPOSITION),
            "source_text_anatomical_site": record.get(
                CONCEPT.BIOSPECIMEN.ANATOMY_SITE
            ),
            "age_at_event_days": record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS),
            "source_text_tumor_descriptor": record.get(
                CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR
            ),
            "ncit_id_tissue_type": record.get(
                CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID
            ),
            "ncit_id_anatomical_site": record.get(
                CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID
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
            "method_of_sample_procurement": record.get(
                CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT
            ),
        }


class GenomicFile:
    class_name = "genomic_file"
    api_path = "/genomic-files"
    target_id_concept = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID
    id_fields = {"kf_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.GENOMIC_FILE.ID]
        return record.get(CONCEPT.GENOMIC_FILE.UNIQUE_KEY) or join(
            record[CONCEPT.GENOMIC_FILE.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(GenomicFile, record),
            "external_id": key,
            "file_name": record.get(CONCEPT.GENOMIC_FILE.FILE_NAME),
            "file_format": record.get(CONCEPT.GENOMIC_FILE.FILE_FORMAT),
            "data_type": record.get(CONCEPT.GENOMIC_FILE.DATA_TYPE),
            "availability": record.get(CONCEPT.GENOMIC_FILE.AVAILABILITY),
            "controlled_access": str_to_obj(
                record.get(CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS)
            ),
            "is_harmonized": record.get(CONCEPT.GENOMIC_FILE.HARMONIZED),
            "hashes": indexd_hashes(record.get(CONCEPT.GENOMIC_FILE.HASH_DICT)),
            "size": int(record.get(CONCEPT.GENOMIC_FILE.SIZE)),
            "urls": str_to_obj(record.get(CONCEPT.GENOMIC_FILE.URL_LIST)),
            "acl": str_to_obj(record.get(CONCEPT.GENOMIC_FILE.ACL)),
            "reference_genome": record.get(
                CONCEPT.GENOMIC_FILE.REFERENCE_GENOME
            ),
            "visible": record.get(CONCEPT.GENOMIC_FILE.VISIBLE),
        }


class ReadGroup:
    class_name = "read_group"
    api_path = "/read-groups"
    target_id_concept = CONCEPT.READ_GROUP.TARGET_SERVICE_ID
    id_fields = {"kf_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.READ_GROUP.ID]
        return record.get(CONCEPT.READ_GROUP.UNIQUE_KEY) or join(
            record[CONCEPT.READ_GROUP.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(ReadGroup, record),
            "external_id": key,
            "flow_cell": record.get(CONCEPT.READ_GROUP.FLOW_CELL),
            "lane_number": record.get(CONCEPT.READ_GROUP.LANE_NUMBER),
            "quality_scale": record.get(CONCEPT.READ_GROUP.QUALITY_SCALE),
            "visible": record.get(CONCEPT.READ_GROUP.VISIBLE),
        }


class SequencingExperiment:
    class_name = "sequencing_experiment"
    api_path = "/sequencing-experiments"
    target_id_concept = CONCEPT.SEQUENCING.TARGET_SERVICE_ID
    id_fields = {"kf_id"}

    @staticmethod
    def build_key(record):
        assert None is not record[CONCEPT.SEQUENCING.ID]
        return record.get(CONCEPT.SEQUENCING.UNIQUE_KEY) or join(
            record[CONCEPT.SEQUENCING.ID]
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(SequencingExperiment, record),
            "sequencing_center_id": record[
                CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
            ],
            "external_id": key,
            "experiment_date": record.get(CONCEPT.SEQUENCING.DATE),
            "experiment_strategy": record.get(CONCEPT.SEQUENCING.STRATEGY),
            "library_name": record.get(CONCEPT.SEQUENCING.LIBRARY_NAME),
            "library_strand": record.get(CONCEPT.SEQUENCING.LIBRARY_STRAND),
            "is_paired_end": record.get(CONCEPT.SEQUENCING.PAIRED_END),
            "platform": record.get(CONCEPT.SEQUENCING.PLATFORM),
            "instrument_model": record.get(CONCEPT.SEQUENCING.INSTRUMENT),
            "max_insert_size": record.get(CONCEPT.SEQUENCING.MAX_INSERT_SIZE),
            "mean_insert_size": record.get(CONCEPT.SEQUENCING.MEAN_INSERT_SIZE),
            "mean_depth": record.get(CONCEPT.SEQUENCING.MEAN_DEPTH),
            "total_reads": record.get(CONCEPT.SEQUENCING.TOTAL_READS),
            "mean_read_length": record.get(CONCEPT.SEQUENCING.MEAN_READ_LENGTH),
            "visible": record.get(CONCEPT.SEQUENCING.VISIBLE),
        }


class FamilyRelationship:
    class_name = "family_relationship"
    api_path = "/family-relationships"
    target_id_concept = CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID
    id_fields = {"kf_id", "participant1_id", "participant2_id"}

    @staticmethod
    def _pid(record, which, get_target_id_from_record):
        return get_target_id_from_record(
            Participant,
            {
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: record.get(
                    which.TARGET_SERVICE_ID
                ),
                CONCEPT.PARTICIPANT.UNIQUE_KEY: record.get(which.UNIQUE_KEY),
                CONCEPT.PARTICIPANT.ID: record.get(which.ID),
            },
        )

    @staticmethod
    def build_key(record):
        p1 = (
            record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID)
            or record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.UNIQUE_KEY)
            or record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID)
        )
        p2 = (
            record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID)
            or record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.UNIQUE_KEY)
            or record.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID)
        )
        assert p1 is not None
        assert p2 is not None
        assert (
            record[CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2] is not None
        )
        return record.get(CONCEPT.FAMILY_RELATIONSHIP.UNIQUE_KEY) or join(
            p1,
            record[
                CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
            ],  # TODO: WE SHOULD REMOVE RELATION_FROM_1_TO_2
            p2,
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(FamilyRelationship, record),
            "participant1_id": FamilyRelationship._pid(
                record,
                CONCEPT.FAMILY_RELATIONSHIP.PERSON1,
                get_target_id_from_record,
            ),
            "participant2_id": FamilyRelationship._pid(
                record,
                CONCEPT.FAMILY_RELATIONSHIP.PERSON2,
                get_target_id_from_record,
            ),
            "external_id": key,
            "participant1_to_participant2_relation": record[
                CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
            ],
            "visible": record.get(CONCEPT.FAMILY_RELATIONSHIP.VISIBLE),
        }


class BiospecimenGenomicFile:
    class_name = "biospecimen_genomic_file"
    api_path = "/biospecimen-genomic-files"
    target_id_concept = CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID
    id_fields = {"kf_id", "biospecimen_id", "genomic_file_id"}

    @staticmethod
    def build_key(record):
        bs = record.get(Biospecimen.target_id_concept) or Biospecimen.build_key(
            record
        )
        gf = record.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            record
        )
        assert None is not bs
        assert None is not gf
        return record.get(CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY) or join(
            bs, gf
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(BiospecimenGenomicFile, record),
            "external_id": key,
            "biospecimen_id": get_target_id_from_record(Biospecimen, record),
            "genomic_file_id": get_target_id_from_record(GenomicFile, record),
            "visible": record.get(CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE),
        }


class BiospecimenDiagnosis:
    class_name = "biospecimen_diagnosis"
    api_path = "/biospecimen-diagnoses"
    target_id_concept = CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID
    id_fields = {"kf_id", "biospecimen_id", "diagnosis_id"}

    @staticmethod
    def build_key(record):
        bs = record.get(Biospecimen.target_id_concept) or Biospecimen.build_key(
            record
        )
        dg = record.get(Diagnosis.target_id_concept) or Diagnosis.build_key(
            record
        )
        assert None is not bs
        assert None is not dg
        return record.get(CONCEPT.BIOSPECIMEN_DIAGNOSIS.UNIQUE_KEY) or join(
            bs, dg
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(BiospecimenDiagnosis, record),
            "external_id": key,
            "biospecimen_id": get_target_id_from_record(Biospecimen, record),
            "diagnosis_id": get_target_id_from_record(Diagnosis, record),
            "visible": record.get(CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE),
        }


class ReadGroupGenomicFile:
    class_name = "read_group_genomic_file"
    api_path = "/read-group-genomic-files"
    target_id_concept = CONCEPT.READ_GROUP_GENOMIC_FILE.TARGET_SERVICE_ID
    id_fields = {"kf_id", "read_group_id", "genomic_file_id"}

    @staticmethod
    def build_key(record):
        rg = record.get(ReadGroup.target_id_concept) or ReadGroup.build_key(
            record
        )
        gf = record.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            record
        )
        assert None is not rg
        assert None is not gf
        return record.get(CONCEPT.READ_GROUP_GENOMIC_FILE.UNIQUE_KEY) or join(
            rg, gf
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(ReadGroupGenomicFile, record),
            "external_id": key,
            "read_group_id": get_target_id_from_record(ReadGroup, record),
            "genomic_file_id": get_target_id_from_record(GenomicFile, record),
            "visible": record.get(CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBLE),
        }


class SequencingExperimentGenomicFile:
    class_name = "sequencing_experiment_genomic_file"
    api_path = "/sequencing-experiment-genomic-files"
    target_id_concept = CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID
    id_fields = {"kf_id", "sequencing_experiment_id", "genomic_file_id"}

    @staticmethod
    def build_key(record):
        se = record.get(
            SequencingExperiment.target_id_concept
        ) or SequencingExperiment.build_key(record)
        gf = record.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            record
        )
        assert None is not se
        assert None is not gf
        return record.get(CONCEPT.SEQUENCING_GENOMIC_FILE.UNIQUE_KEY) or join(
            se, gf
        )

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        return {
            "kf_id": get_target_id_from_record(
                SequencingExperimentGenomicFile, record
            ),
            "external_id": key,
            "sequencing_experiment_id": get_target_id_from_record(
                SequencingExperiment, record
            ),
            "genomic_file_id": get_target_id_from_record(GenomicFile, record),
            "visible": record.get(CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE),
        }


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


def coerce_types(host, entity_class, body):
    if not swagger_cache:
        swagger = get_open_api_v2_schema(host)
        defs = swagger["definitions"]
        for c in all_targets:
            n = c.class_name
            uccn = upper_camel_case(n)
            if uccn in defs:
                swagger_cache[n] = defs[uccn]

    properties = swagger_cache[entity_class.class_name]["properties"]

    ret = {}
    for k, v in body.items():
        if properties[k].get("readOnly"):  # e.g. modified_at/created_at
            continue
        elif (v is None) or (v == ""):
            if properties[k]["type"] == "string":
                if "date" in k:  # TODO: FIX THE DATASERVICE?
                    ret[k] = None
                elif k not in entity_class.id_fields:
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
            ret[k] = json_type_casts.get(properties[k]["type"], lambda x: x)(v)

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
        result = resp.json()["results"]
        return result["kf_id"]
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
            f"Sent to {api_path}:\n{body}\nGot:\n{resp.text}"
        )
