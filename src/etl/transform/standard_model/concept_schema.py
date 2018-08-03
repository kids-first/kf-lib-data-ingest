DELIMITER = '|'


class CONCEPT:

    class INVESTIGATOR:
        ID = 'INVESTIGATOR|ID'
        NAME = 'INVESTIGATOR_NAME'
        INSTITUTION = 'INVESTIGATOR_INSTITUTION'

    class STUDY:
        KF_ID = 'STUDY|KF_ID'
        ID = 'STUDY|ID'
        AUTHORITY = 'STUDY|AUTHORITY'
        VERSION = 'STUDY|VERSION'
        NAME = 'STUDY|NAME'
        SHORT_NAME = 'STUDY|SHORT_NAME'
        ATTRIBUTION = 'STUDY|ATTRIBUTION'
        RELEASE_STATUS = 'STUDY|RELEASE_STATUS'
        CATEGORY = 'STUDY|CATEGORY'

    class FAMILY:
        ID = 'FAMILY|ID'

    class PARTICIPANT:
        ID = 'PARTICIPANT|ID'
        FAMILY = 'PARTICIPANT|FAMILY'
        IS_PROBAND = 'PARTICIPANT|IS_PROBAND'
        FATHER_ID = 'PARTICIPANT|FATHER_ID'
        MOTHER_ID = 'PARTICIPANT|MOTHER_ID'
        GENDER = 'PARTICIPANT|GENDER'
        ETHNICITY = 'PARTICIPANT|ETHNICITY'
        RACE = 'PARTICIPANT|RACE'
        CONSENT_TYPE = 'PARTICIPANT|CONSENT_TYPE'

    class OUTCOME:
        ID = 'OUTCOME|ID'
        VITAL_STATUS = 'OUTCOME|VITAL_STATUS'
        EVENT_AGE = 'OUTCOME|EVENT_AGE'
        RELATED = 'OUTCOME|RELATED'

    class DIAGNOSIS:
        ID = 'DIAGNOSIS|ID'
        NAME = 'DIAGNOSIS|NAME'
        TUMOR_LOCATION = 'DIAGNOSIS|TUMOR_LOCATION'
        UBERON_TUMOR_LOCATION_ID = 'DIAGNOSIS|UBERON_TUMOR_LOCATION_ID'
        EVENT_AGE = 'DIAGNOSIS|EVENT_AGE'
        MONDO_ID = 'DIAGNOSIS|MONDO_ID'
        NCIT_ID = 'DIAGNOSIS|NCIT_ID'
        ICD_ID = 'DIAGNOSIS|ICD_ID'

    class PHENOTYPE:
        ID = 'PHENOTYPE|ID'
        NAME = 'PHENOTYPE|NAME'
        HPO_ID = 'PHENOTYPE|HPO_ID'
        SNOMED_ID = 'PHENOTYPE|SNOMED_ID'
        OBSERVED = 'PHENOTYPE|OBSERVED'
        EVENT_AGE = 'PHENOTYPE|EVENT_AGE'

    class BIOSPECIMEN:
        KF_ID = 'BIOSPECIMEN|KF_ID'
        ID = 'BIOSPECIMEN|ID'
        ALIQUOT_ID = 'BIOSPECIMEN|ALIQUOT_ID'
        TISSUE_TYPE = 'BIOSPECIMEN|TISSUE_TYPE'
        NCIT_TISSUE_TYPE_ID = 'BIOSPECIMEN|NCIT_TISSUE_TYPE_ID'
        ANATOMY_SITE = 'BIOSPECIMEN|ANATOMY_SITE'
        NCIT_ANATOMY_SITE_ID = 'BIOSPECIMEN|NCIT_ANATOMY_SITE_ID'
        UBERON_ANATOMY_SITE_ID = 'BIOSPECIMEN|UBERON_ANATOMY_SITE_ID'
        TUMOR_DESCRIPTOR = 'BIOSPECIMEN|TUMOR_DESCRIPTOR'
        COMPOSITION = 'BIOSPECIMEN|COMPOSITION'
        EVENT_AGE = 'BIOSPECIMEN|EVENT_AGE'
        SPATIAL_DESCRIPTOR = 'BIOSPECIMEN|SPATIAL_DESCRIPTOR'
        SHIPMENT_ORIGIN = 'BIOSPECIMEN|SHIPMENT_ORIGIN'
        SHIPMENT_DATE = 'BIOSPECIMEN|SHIPMENT_DATE'
        ANALYTE = 'BIOSPECIMEN|ANALYTE'
        CONCENTRATION_MG_PER_ML = 'BIOSPECIMEN|CONCENTRATION_MG_PER_ML'
        VOLUME_ML = 'BIOSPECIMEN|VOLUME_ML'

    class GENOMIC_FILE:
        KF_ID = 'GENOMIC_FILE|KF_ID'
        ETAG = 'GENOMIC_FILE|ETAG'
        SIZE = 'GENOMIC_FILE|SIZE'
        ID = 'GENOMIC_FILE|ID'
        FILE_NAME = 'GENOMIC_FILE|NAME'
        FILE_PATH = 'GENOMIC_FILE|PATH'
        HARMONIZED = 'GENOMIC_FILE|HARMONIZED'
        CAVATICA_OUTPUT_FILE = 'GENOMIC_FILE|CAVATICA_OUTPUT_FILE'

    class MULTI_SPECIMEN_GENOMIC_FILE(GENOMIC_FILE):
        ID = 'MULTI_SPECIMEN_GENOMIC_FILE|ID'

    class SEQUENCING:
        ID = 'SEQUENCING|ID'
        DATE = 'SEQUENCING|DATE'
        STRATEGY = 'SEQUENCING|STRATEGY'
        PAIRED_END = 'SEQUENCING|IS_PAIRED_END'
        LIBRARY_NAME = 'SEQUENCING|LIBRARY_NAME'
        LIBRARY_STRAND = 'SEQUENCING|LIBRARY_STRAND'
        PLATFORM = 'SEQUENCING|PLATFORM'
        INSTRUMENT = 'SEQUENCING|INSTRUMENT'
        INSERT_SIZE = 'SEQUENCING|INSERT_SIZE'
        REFERENCE_GENOME = 'SEQUENCING|REFERENCE_GENOME'
        MAX_INSERT_SIZE = 'SEQUENCING|MAX_INSERT_SIZE'
        MEAN_INSERT_SIZE = 'SEQUENCING|MEAN_INSERT_SIZE'
        MEAN_DEPTH = 'SEQUENCING|MEAN_DEPTH'
        TOTAL_READS = 'SEQUENCING|TOTAL_READS'
        MEAN_READ_LENGTH = 'SEQUENCING|MEAN_READ_LENGTH'

        class CENTER:
            NAME = 'SEQUENCING_CENTER|NAME'
            KF_ID = 'SEQUENCING_CENTER|KF_ID'


concept_set = {CONCEPT.FAMILY,
               CONCEPT.SEQUENCING,
               CONCEPT.PARTICIPANT,
               CONCEPT.BIOSPECIMEN,
               CONCEPT.DIAGNOSIS,
               CONCEPT.PHENOTYPE,
               CONCEPT.DIAGNOSIS,
               CONCEPT.OUTCOME,
               CONCEPT.GENOMIC_FILE
               }


class ConceptSchemaValidationError(Exception):
    pass


def _create_identifiers():
    """
    Build the set of identifying concept properties.

    - All CONCEPT.<>.ID properties are identifiers
    - Some concepts have additional identifying properties
    """
    # Add all ID properties of each concept
    identifiers = {}
    for concept in concept_set:
        identifiers.setdefault(concept.__name__, set())
        identifiers[concept.__name__].add(str(concept.ID))

    # Add other misc identifiers
    misc = [(CONCEPT.GENOMIC_FILE, CONCEPT.GENOMIC_FILE.FILE_PATH),
            (CONCEPT.SEQUENCING, CONCEPT.SEQUENCING.LIBRARY_NAME),
            (CONCEPT.BIOSPECIMEN, CONCEPT.BIOSPECIMEN.ALIQUOT_ID)]
    for tup in misc:
        identifiers[tup[0].__name__].add(str(tup[1]))

    return identifiers


identifiers = _create_identifiers()


def validate_concept_string(concept_property_string):
    """
    Given a delimited concept property string check whether the property
    exists in the defined hierarchy of standard concepts and properties.

    :param concept_property_string: a string like: SEQUENCING_CENTER|NAME
    """
    hierarchy = concept_property_string.split(DELIMITER)
    if hierarchy[0] == CONCEPT.__name__:
        hierarchy = hierarchy[1:]
    prev = CONCEPT
    for node in hierarchy:
        try:
            prev = getattr(prev, node.strip().upper())
        except AttributeError:
            raise ConceptSchemaValidationError(
                '{} does not exist in standard concepts'
                .format(concept_property_string))
    return True


def is_identifier(concept_property_string):
    """
    Given a delimited concept property string check whether the property
    is an identifying property for this concept.

    :param concept_property_string: a string like: PARTICIPANT|ID
    """
    # Raise exception if the concept property does not exist
    validate_concept_string(concept_property_string)

    # Construct the set of all identifier properties
    identifier_strings = {
        value
        for concept, set_of_values in identifiers.items()
        for value in set_of_values
    }

    return (concept_property_string in identifier_strings)
