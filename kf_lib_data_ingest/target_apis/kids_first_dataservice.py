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
from kf_lib_data_ingest.common.misc import str_to_obj


def indexd_hashes(dictstr):
    return {
        k.lower().replace("-", ""): v for k, v in str_to_obj(dictstr).items()
    }


def join(*args):
    """
    Joins args using periods.
    This is used when making compound unique keys from row data.

    :return: `".".join([str(a) for a in args])`
    :rtype: str
    """
    return "|".join([str(a) for a in args])


def get_target_id(entity_class, row, target_id_lookup_func):
    """
    Find the target service ID for the given row and entity class.

    :param entity_class: one of the classes contained in the all_targets list
    :type entity_class: class
    :param row: a row of extracted data
    :type row: dict
    :param target_id_lookup_func: a function that takes a class name and a lookup key
        and returns a target service ID based on those arguments if one has been cached
    :type target_id_lookup_func: function
    :return: the target service ID
    :rtype: str
    """
    tic = row.get(entity_class.target_id_concept)

    if tic and (tic != constants.COMMON.NOT_REPORTED):
        return tic
    else:
        try:
            return target_id_lookup_func(
                entity_class.class_name, entity_class.build_key(row)
            )
        except AssertionError:
            return None


def without_nulls(body):
    # Remove elements with null values
    return {k: v for k, v in body.items() if v is not None}


class Investigator:
    class_name = "investigator"
    api_path = "/investigators"
    target_id_concept = CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.INVESTIGATOR.NAME]
        assert None is not row[CONCEPT.INVESTIGATOR.INSTITUTION]
        return row.get(CONCEPT.INVESTIGATOR.UNIQUE_KEY) or join(
            row[CONCEPT.INVESTIGATOR.NAME],
            row[CONCEPT.INVESTIGATOR.INSTITUTION],
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    Investigator, row, target_id_lookup_func
                ),
                "external_id": key,
                "name": row.get(CONCEPT.INVESTIGATOR.NAME),
                "institution": row.get(CONCEPT.INVESTIGATOR.INSTITUTION),
                "visible": row.get(CONCEPT.INVESTIGATOR.VISIBLE),
            }
        )


class Study:
    class_name = "study"
    api_path = "/studies"
    target_id_concept = CONCEPT.STUDY.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.STUDY.ID]
        return row.get(CONCEPT.STUDY.UNIQUE_KEY) or join(row[CONCEPT.STUDY.ID])

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Study, row, target_id_lookup_func),
                "external_id": key,
                "investigator_id": get_target_id(
                    Investigator, row, target_id_lookup_func
                ),
                "name": row.get(CONCEPT.STUDY.NAME),
                "short_name": row.get(CONCEPT.STUDY.SHORT_NAME),
                "version": row.get(CONCEPT.STUDY.VERSION),
                "data_access_authority": row.get(CONCEPT.STUDY.AUTHORITY),
                "release_status": row.get(CONCEPT.STUDY.RELEASE_STATUS),
                "attribution": row.get(CONCEPT.STUDY.ATTRIBUTION),
                "category": row.get(CONCEPT.STUDY.CATEGORY),
                "visible": row.get(CONCEPT.STUDY.VISIBLE),
            }
        )


class Family:
    class_name = "family"
    api_path = "/families"
    target_id_concept = CONCEPT.FAMILY.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.FAMILY.ID]
        return row.get(CONCEPT.FAMILY.UNIQUE_KEY) or join(
            row[CONCEPT.FAMILY.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Family, row, target_id_lookup_func),
                "external_id": key,
                "visible": row.get(CONCEPT.FAMILY.VISIBLE),
            }
        )


class Participant:
    class_name = "participant"
    api_path = "/participants"
    target_id_concept = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.PARTICIPANT.ID]
        return row.get(CONCEPT.PARTICIPANT.UNIQUE_KEY) or join(
            row[CONCEPT.PARTICIPANT.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Participant, row, target_id_lookup_func),
                "study_id": row[CONCEPT.STUDY.ID],
                "family_id": get_target_id(Family, row, target_id_lookup_func),
                "external_id": key,
                "is_proband": row.get(CONCEPT.PARTICIPANT.IS_PROBAND),
                "ethnicity": row.get(CONCEPT.PARTICIPANT.ETHNICITY),
                "gender": row.get(CONCEPT.PARTICIPANT.GENDER),
                "race": row.get(CONCEPT.PARTICIPANT.RACE),
                "affected_status": row.get(
                    CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY
                ),
                "species": row.get(CONCEPT.PARTICIPANT.SPECIES),
                "visible": row.get(CONCEPT.PARTICIPANT.VISIBLE),
            }
        )


class Diagnosis:
    class_name = "diagnosis"
    api_path = "/diagnoses"
    target_id_concept = CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.PARTICIPANT.ID]
        assert None is not row[CONCEPT.DIAGNOSIS.NAME]
        return row.get(CONCEPT.DIAGNOSIS.UNIQUE_KEY) or join(
            row[CONCEPT.PARTICIPANT.ID],
            row[CONCEPT.DIAGNOSIS.NAME],
            row.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Diagnosis, row, target_id_lookup_func),
                "participant_id": get_target_id(
                    Participant, row, target_id_lookup_func
                ),
                "external_id": key,
                "age_at_event_days": row.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS),
                "source_text_diagnosis": row.get(CONCEPT.DIAGNOSIS.NAME),
                "source_text_tumor_location": row.get(
                    CONCEPT.DIAGNOSIS.TUMOR_LOCATION
                ),
                "mondo_id_diagnosis": row.get(CONCEPT.DIAGNOSIS.MONDO_ID),
                "icd_id_diagnosis": row.get(CONCEPT.DIAGNOSIS.ICD_ID),
                "uberon_id_tumor_location": row.get(
                    CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID
                ),
                "ncit_id_diagnosis": row.get(CONCEPT.DIAGNOSIS.NCIT_ID),
                "spatial_descriptor": row.get(
                    CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR
                ),
                "diagnosis_category": row.get(CONCEPT.DIAGNOSIS.CATEGORY),
                "visible": row.get(CONCEPT.DIAGNOSIS.VISIBLE),
            }
        )


class Phenotype:
    class_name = "phenotype"
    api_path = "/phenotypes"
    target_id_concept = CONCEPT.PHENOTYPE.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.PARTICIPANT.ID]
        assert None is not row[CONCEPT.PHENOTYPE.NAME]
        assert row[CONCEPT.PHENOTYPE.OBSERVED] in {
            constants.PHENOTYPE.OBSERVED.NO,
            constants.PHENOTYPE.OBSERVED.YES,
        }
        return row.get(CONCEPT.PHENOTYPE.UNIQUE_KEY) or join(
            row[CONCEPT.PARTICIPANT.ID],
            row[CONCEPT.PHENOTYPE.NAME],
            row[CONCEPT.PHENOTYPE.OBSERVED],  # TODO: WE SHOULD REMOVE OBSERVED
            row.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Phenotype, row, target_id_lookup_func),
                "participant_id": get_target_id(
                    Participant, row, target_id_lookup_func
                ),
                "external_id": key,
                "age_at_event_days": row.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS),
                "source_text_phenotype": row.get(CONCEPT.PHENOTYPE.NAME),
                "hpo_id_phenotype": row.get(CONCEPT.PHENOTYPE.HPO_ID),
                "snomed_id_phenotype": row.get(CONCEPT.PHENOTYPE.SNOMED_ID),
                "observed": row.get(CONCEPT.PHENOTYPE.OBSERVED),
                "visible": row.get(CONCEPT.PHENOTYPE.VISIBLE),
            }
        )


class Outcome:
    class_name = "outcome"
    api_path = "/outcomes"
    target_id_concept = CONCEPT.OUTCOME.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.PARTICIPANT.ID]
        assert row[CONCEPT.OUTCOME.VITAL_STATUS] in {
            constants.OUTCOME.VITAL_STATUS.ALIVE,
            constants.OUTCOME.VITAL_STATUS.DEAD,
        }
        return row.get(CONCEPT.OUTCOME.UNIQUE_KEY) or join(
            row[CONCEPT.PARTICIPANT.ID],
            row[CONCEPT.OUTCOME.VITAL_STATUS],
            row.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS),
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Outcome, row, target_id_lookup_func),
                "participant_id": get_target_id(
                    Participant, row, target_id_lookup_func
                ),
                "external_id": key,
                "age_at_event_days": row.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS),
                "vital_status": row.get(CONCEPT.OUTCOME.VITAL_STATUS),
                "disease_related": row.get(CONCEPT.OUTCOME.DISEASE_RELATED),
                "visible": row.get(CONCEPT.OUTCOME.VISIBLE),
            }
        )


class Biospecimen:
    class_name = "biospecimen"
    api_path = "/biospecimens"
    target_id_concept = CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.BIOSPECIMEN_GROUP.ID]
        assert None is not row[CONCEPT.BIOSPECIMEN.ID]
        assert row[CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID]
        return row.get(CONCEPT.BIOSPECIMEN.UNIQUE_KEY) or join(
            row[CONCEPT.BIOSPECIMEN_GROUP.ID], row[CONCEPT.BIOSPECIMEN.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(Biospecimen, row, target_id_lookup_func),
                "participant_id": get_target_id(
                    Participant, row, target_id_lookup_func
                ),
                "external_sample_id": row.get(CONCEPT.BIOSPECIMEN_GROUP.ID),
                "external_aliquot_id": row.get(CONCEPT.BIOSPECIMEN.ID),
                "sequencing_center_id": row.get(
                    CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
                ),
                "source_text_tissue_type": row.get(
                    CONCEPT.BIOSPECIMEN.TISSUE_TYPE
                ),
                "composition": row.get(CONCEPT.BIOSPECIMEN.COMPOSITION),
                "source_text_anatomical_site": row.get(
                    CONCEPT.BIOSPECIMEN.ANATOMY_SITE
                ),
                "age_at_event_days": row.get(
                    CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS
                ),
                "source_text_tumor_descriptor": row.get(
                    CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR
                ),
                "ncit_id_tissue_type": row.get(
                    CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID
                ),
                "ncit_id_anatomical_site": row.get(
                    CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID
                ),
                "spatial_descriptor": row.get(
                    CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR
                ),
                "shipment_origin": row.get(CONCEPT.BIOSPECIMEN.SHIPMENT_ORIGIN),
                "shipment_date": row.get(CONCEPT.BIOSPECIMEN.SHIPMENT_DATE),
                "analyte_type": row.get(CONCEPT.BIOSPECIMEN.ANALYTE),
                "concentration_mg_per_ml": row.get(
                    CONCEPT.BIOSPECIMEN.CONCENTRATION_MG_PER_ML
                ),
                "volume_ul": row.get(CONCEPT.BIOSPECIMEN.VOLUME_UL),
                "visible": row.get(CONCEPT.BIOSPECIMEN.VISIBLE),
                "method_of_sample_procurement": row.get(
                    CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT
                ),
            }
        )


class GenomicFile:
    class_name = "genomic_file"
    api_path = "/genomic-files"
    target_id_concept = CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.GENOMIC_FILE.ID]
        return row.get(CONCEPT.GENOMIC_FILE.UNIQUE_KEY) or join(
            row[CONCEPT.GENOMIC_FILE.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(GenomicFile, row, target_id_lookup_func),
                "external_id": key,
                "file_name": row.get(CONCEPT.GENOMIC_FILE.FILE_NAME),
                "file_format": row.get(CONCEPT.GENOMIC_FILE.FILE_FORMAT),
                "data_type": row.get(CONCEPT.GENOMIC_FILE.DATA_TYPE),
                "availability": row.get(CONCEPT.GENOMIC_FILE.AVAILABILITY),
                "controlled_access": str_to_obj(
                    row.get(CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS)
                ),
                "is_harmonized": row.get(CONCEPT.GENOMIC_FILE.HARMONIZED),
                "hashes": indexd_hashes(
                    row.get(CONCEPT.GENOMIC_FILE.HASH_DICT)
                ),
                "size": int(row.get(CONCEPT.GENOMIC_FILE.SIZE)),
                "urls": str_to_obj(row.get(CONCEPT.GENOMIC_FILE.URL_LIST)),
                "acl": str_to_obj(row.get(CONCEPT.GENOMIC_FILE.ACL)),
                "reference_genome": row.get(
                    CONCEPT.GENOMIC_FILE.REFERENCE_GENOME
                ),
                "visible": row.get(CONCEPT.GENOMIC_FILE.VISIBLE),
            }
        )


class ReadGroup:
    class_name = "read_group"
    api_path = "/read-groups"
    target_id_concept = CONCEPT.READ_GROUP.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.READ_GROUP.ID]
        return row.get(CONCEPT.READ_GROUP.UNIQUE_KEY) or join(
            row[CONCEPT.READ_GROUP.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(ReadGroup, row, target_id_lookup_func),
                "external_id": key,
                "flow_cell": row.get(CONCEPT.READ_GROUP.FLOW_CELL),
                "lane_number": row.get(CONCEPT.READ_GROUP.LANE_NUMBER),
                "quality_scale": row.get(CONCEPT.READ_GROUP.QUALITY_SCALE),
                "visible": row.get(CONCEPT.READ_GROUP.VISIBLE),
            }
        )


class SequencingExperiment:
    class_name = "sequencing_experiment"
    api_path = "/sequencing-experiments"
    target_id_concept = CONCEPT.SEQUENCING.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        assert None is not row[CONCEPT.SEQUENCING.ID]
        return row.get(CONCEPT.SEQUENCING.UNIQUE_KEY) or join(
            row[CONCEPT.SEQUENCING.ID]
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    SequencingExperiment, row, target_id_lookup_func
                ),
                "sequencing_center_id": row[
                    CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID
                ],
                "external_id": key,
                "experiment_date": row.get(CONCEPT.SEQUENCING.DATE),
                "experiment_strategy": row.get(CONCEPT.SEQUENCING.STRATEGY),
                "library_name": row.get(CONCEPT.SEQUENCING.LIBRARY_NAME),
                "library_strand": row.get(CONCEPT.SEQUENCING.LIBRARY_STRAND),
                "is_paired_end": row.get(CONCEPT.SEQUENCING.PAIRED_END),
                "platform": row.get(CONCEPT.SEQUENCING.PLATFORM),
                "instrument_model": row.get(CONCEPT.SEQUENCING.INSTRUMENT),
                "max_insert_size": row.get(CONCEPT.SEQUENCING.MAX_INSERT_SIZE),
                "mean_insert_size": row.get(
                    CONCEPT.SEQUENCING.MEAN_INSERT_SIZE
                ),
                "mean_depth": row.get(CONCEPT.SEQUENCING.MEAN_DEPTH),
                "total_reads": row.get(CONCEPT.SEQUENCING.TOTAL_READS),
                "mean_read_length": row.get(
                    CONCEPT.SEQUENCING.MEAN_READ_LENGTH
                ),
                "visible": row.get(CONCEPT.SEQUENCING.VISIBLE),
            }
        )


class FamilyRelationship:
    class_name = "family_relationship"
    api_path = "/family-relationships"
    target_id_concept = CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID

    @staticmethod
    def _pid(row, which, target_id_lookup_func):
        return row.get(which.TARGET_SERVICE_ID) or target_id_lookup_func(
            Participant.class_name,
            row.get(which.UNIQUE_KEY) or row.get(which.ID),
        )

    @staticmethod
    def build_key(row):
        p1 = (
            row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID)
            or row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.UNIQUE_KEY)
            or row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON1.ID)
        )
        p2 = (
            row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID)
            or row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.UNIQUE_KEY)
            or row.get(CONCEPT.FAMILY_RELATIONSHIP.PERSON2.ID)
        )
        assert None is not p1
        assert None is not p2
        assert None is not row[CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2]
        return row.get(CONCEPT.FAMILY_RELATIONSHIP.UNIQUE_KEY) or join(
            p1,
            row[
                CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
            ],  # TODO: WE SHOULD REMOVE RELATION_FROM_1_TO_2
            p2,
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    FamilyRelationship, row, target_id_lookup_func
                ),
                "participant1_id": FamilyRelationship._pid(
                    row,
                    CONCEPT.FAMILY_RELATIONSHIP.PERSON1,
                    target_id_lookup_func,
                ),
                "participant2_id": FamilyRelationship._pid(
                    row,
                    CONCEPT.FAMILY_RELATIONSHIP.PERSON2,
                    target_id_lookup_func,
                ),
                "external_id": key,
                "participant1_to_participant2_relation": row[
                    CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2
                ],
                "visible": row.get(CONCEPT.FAMILY_RELATIONSHIP.VISIBLE),
            }
        )


class BiospecimenGenomicFile:
    class_name = "biospecimen_genomic_file"
    api_path = "/biospecimen-genomic-files"
    target_id_concept = CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        bs = row.get(Biospecimen.target_id_concept) or Biospecimen.build_key(
            row
        )
        gf = row.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            row
        )
        assert None is not bs
        assert None is not gf
        return row.get(CONCEPT.BIOSPECIMEN_GENOMIC_FILE.UNIQUE_KEY) or join(
            bs, gf
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    BiospecimenGenomicFile, row, target_id_lookup_func
                ),
                "external_id": key,
                "biospecimen_id": get_target_id(
                    Biospecimen, row, target_id_lookup_func
                ),
                "genomic_file_id": get_target_id(
                    GenomicFile, row, target_id_lookup_func
                ),
                "visible": row.get(CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE),
            }
        )


class BiospecimenDiagnosis:
    class_name = "biospecimen_diagnosis"
    api_path = "/biospecimen-diagnoses"
    target_id_concept = CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        bs = row.get(Biospecimen.target_id_concept) or Biospecimen.build_key(
            row
        )
        dg = row.get(Diagnosis.target_id_concept) or Diagnosis.build_key(row)
        assert None is not bs
        assert None is not dg
        return row.get(CONCEPT.BIOSPECIMEN_DIAGNOSIS.UNIQUE_KEY) or join(bs, dg)

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    BiospecimenDiagnosis, row, target_id_lookup_func
                ),
                "external_id": key,
                "biospecimen_id": get_target_id(
                    Biospecimen, row, target_id_lookup_func
                ),
                "diagnosis_id": get_target_id(
                    Diagnosis, row, target_id_lookup_func
                ),
                "visible": row.get(CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE),
            }
        )


class ReadGroupGenomicFile:
    class_name = "read_group_genomic_file"
    api_path = "/read-group-genomic-files"
    target_id_concept = CONCEPT.READ_GROUP_GENOMIC_FILE.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        rg = row.get(ReadGroup.target_id_concept) or ReadGroup.build_key(row)
        gf = row.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            row
        )
        assert None is not rg
        assert None is not gf
        return row.get(CONCEPT.READ_GROUP_GENOMIC_FILE.UNIQUE_KEY) or join(
            rg, gf
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    ReadGroupGenomicFile, row, target_id_lookup_func
                ),
                "external_id": key,
                "read_group_id": get_target_id(
                    ReadGroup, row, target_id_lookup_func
                ),
                "genomic_file_id": get_target_id(
                    GenomicFile, row, target_id_lookup_func
                ),
                "visible": row.get(CONCEPT.READ_GROUP_GENOMIC_FILE.VISIBLE),
            }
        )


class SequencingExperimentGenomicFile:
    class_name = "sequencing_experiment_genomic_file"
    api_path = "/sequencing-experiment-genomic-files"
    target_id_concept = CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID

    @staticmethod
    def build_key(row):
        se = row.get(
            SequencingExperiment.target_id_concept
        ) or SequencingExperiment.build_key(row)
        gf = row.get(GenomicFile.target_id_concept) or GenomicFile.build_key(
            row
        )
        assert None is not se
        assert None is not gf
        return row.get(CONCEPT.SEQUENCING_GENOMIC_FILE.UNIQUE_KEY) or join(
            se, gf
        )

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        return without_nulls(
            {
                "kf_id": get_target_id(
                    SequencingExperimentGenomicFile, row, target_id_lookup_func
                ),
                "external_id": key,
                "sequencing_experiment_id": get_target_id(
                    SequencingExperiment, row, target_id_lookup_func
                ),
                "genomic_file_id": get_target_id(
                    GenomicFile, row, target_id_lookup_func
                ),
                "visible": row.get(CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE),
            }
        )


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

    if kf_id:
        resp = _PATCH(host, api_path, kf_id, body)
        if resp.status_code == 404:
            resp = None

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
