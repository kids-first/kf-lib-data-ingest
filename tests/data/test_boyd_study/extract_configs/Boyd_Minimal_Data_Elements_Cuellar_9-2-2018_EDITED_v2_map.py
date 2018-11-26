import pandas

from kf_lib_data_ingest.common import constants, file_retriever
from kf_lib_data_ingest.etl.extract.operations import *
from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
    CONCEPT
)

source_data_url = (
    's3://kf-study-us-east-1-prd-sd-p445achv/study-files/modified/'
    'Boyd_Minimal_Data_Elements_Cuellar_9-2-2018_EDITED_v2.tsv'
)

source_data_loading_parameters = {}


# Boyd re-issued new samples without updating their data table with the new
# sample IDs, so we track the mappings from the sample manifest and then apply
# them here when extracting the sample/aliquot IDs.
_manifest_data = pandas.read_table(
    file_retriever.FileRetriever().get(
        's3://kf-study-us-east-1-prd-sd-p445achv/study-files/modified/'
        'manifest_180917_rectified.tsv'
    )
)
_sample_mapping = {
    v['Sample Description'][10:]: v['Sample Name']
    for _, v in _manifest_data.iterrows()
    if v['Sample Description'].startswith('germline: 5136-SB-')
}


operations = [
    value_map(
        in_col='Sample/Aliquot ID',
        m={
            r'.*': lambda x: _sample_mapping.get(x, x)
        },
        out_col=CONCEPT.BIOSPECIMEN.ID
    ),
    value_map(
        in_col='Sample/Aliquot ID',
        m={
            r'.*': lambda x: _sample_mapping.get(x, x)
        },
        out_col=CONCEPT.BIOSPECIMEN.ALIQUOT_ID
    ),
    keep_map(
        in_col='Participant ID',
        out_col=CONCEPT.PARTICIPANT.ID
    ),
    keep_map(
        in_col='Family Group ID',
        out_col=CONCEPT.FAMILY.ID
    ),
    value_map(
        m=lambda x: x not in {'1333', '2044'},  # these families are mangled
        in_col='Family Group ID',
        out_col=CONCEPT.FAMILY.HIDDEN
    ),
    constant_map(
        m=constants.SPECIMEN.TISSUE_TYPE.GERMLINE,
        out_col=CONCEPT.BIOSPECIMEN.TISSUE_TYPE
    ),
    value_map(
        m={
            r'osteo': constants.SPECIMEN.COMPOSITION.BONE,
            r'': constants.SPECIMEN.COMPOSITION.BLOOD
        },
        in_col='Tissue from notation',
        out_col=CONCEPT.BIOSPECIMEN.COMPOSITION
    ),
    value_map(
        m={
            r'osteo': constants.SPECIMEN.ANATOMY_SITE.SKULL
        },
        in_col='Tissue from notation',
        out_col=CONCEPT.BIOSPECIMEN.ANATOMY_SITE
    ),
    value_map(
        m={
            r'Proband': constants.RELATIONSHIP.PROBAND,
            r'Mother': constants.RELATIONSHIP.MOTHER,
            r'Father': constants.RELATIONSHIP.FATHER,
            r'Brother': constants.RELATIONSHIP.BROTHER,
            r'Sister': constants.RELATIONSHIP.SISTER,
            r'Spouse': constants.RELATIONSHIP.SPOUSE,
            r'Daughter': constants.RELATIONSHIP.DAUGHTER,
            r'Son': constants.RELATIONSHIP.SON,
            r'Twin brother': constants.RELATIONSHIP.TWIN_BROTHER,
            r'Twin sister': constants.RELATIONSHIP.TWIN_SISTER,
            r'Maternal Grandmother':
                constants.RELATIONSHIP.MATERNAL_GRANDMOTHER,
            r'Grand daughter': constants.RELATIONSHIP.GRANDDAUGHTER
        },
        in_col='Family Relationship',
        out_col=CONCEPT.PARTICIPANT.RELATIONSHIP_TO_PROBAND
    ),
    value_map(
        m={
            r'[F|f]emale': constants.GENDER.FEMALE,
            r'[M|m]ale': constants.GENDER.MALE
        },
        in_col='Sex',
        out_col=CONCEPT.PARTICIPANT.GENDER
    ),
    value_map(
        m={
            r'[B|b]lack': constants.RACE.BLACK,
            r'[W|w]hite': constants.RACE.WHITE,
            r'[A|a]sian': constants.RACE.ASIAN
        },
        in_col='Race',
        out_col=CONCEPT.PARTICIPANT.RACE
    ),
    keep_map(
        in_col='Ethnicity',
        out_col=CONCEPT.PARTICIPANT.ETHNICITY
    ),
    keep_map(
        in_col='Phenotypes Text',
        out_col=CONCEPT.PHENOTYPE.NAME
    ),
    keep_map(
        in_col='Phenotypes HPO',
        out_col=CONCEPT.PHENOTYPE.HPO_ID
    ),
    keep_map(
        in_col='Diagnoses Text',
        out_col=CONCEPT.DIAGNOSIS.NAME
    ),
    value_map(
        m={
            r'(.+)': lambda x: 'NCIT:' + x
        },
        in_col='Diagnosis NCIT',
        out_col=CONCEPT.DIAGNOSIS.NCIT_ID
    ),
    keep_map(
        in_col='Vital Status',
        out_col=CONCEPT.OUTCOME.VITAL_STATUS
    )
]
