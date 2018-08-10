DELIMITER = '|'


class CONCEPT:

    class INVESTIGATOR:
        ID = None
        NAME = None
        INSTITUTION = None

    class STUDY:
        ID = None
        AUTHORITY = None
        VERSION = None
        NAME = None
        SHORT_NAME = None
        ATTRIBUTION = None
        RELEASE_STATUS = None
        CATEGORY = None

    class FAMILY:
        ID = None

    class PARTICIPANT:
        ID = None
        FAMILY = None
        IS_PROBAND = None
        FATHER_ID = None
        MOTHER_ID = None
        GENDER = None
        ETHNICITY = None
        RACE = None
        CONSENT_TYPE = None

    class OUTCOME:
        ID = None
        VITAL_STATUS = None
        EVENT_AGE_DAYS = None
        DISEASE_RELATED = None

    class DIAGNOSIS:
        ID = None
        NAME = None
        TUMOR_LOCATION = None
        SPATIAL_DESCRIPTOR = None
        CATEGORY = None
        UBERON_TUMOR_LOCATION_ID = None
        EVENT_AGE_DAYS = None
        MONDO_ID = None
        NCIT_ID = None
        ICD_ID = None

    class PHENOTYPE:
        ID = None
        NAME = None
        HPO_ID = None
        SNOMED_ID = None
        OBSERVED = None
        EVENT_AGE_DAYS = None

    class BIOSPECIMEN:
        ID = None
        ALIQUOT_ID = None
        TISSUE_TYPE = None
        NCIT_TISSUE_TYPE_ID = None
        ANATOMY_SITE = None
        NCIT_ANATOMY_SITE_ID = None
        UBERON_ANATOMY_SITE_ID = None
        TUMOR_DESCRIPTOR = None
        COMPOSITION = None
        EVENT_AGE_DAYS = None
        SPATIAL_DESCRIPTOR = None
        SHIPMENT_ORIGIN = None
        SHIPMENT_DATE = None
        ANALYTE = None
        CONCENTRATION_MG_PER_ML = None
        VOLUME_ML = None

    class GENOMIC_FILE:
        ETAG = None
        SIZE = None
        ID = None
        FILE_NAME = None
        FILE_PATH = None
        HARMONIZED = None
        CAVATICA_OUTPUT_FILE = None

    class MULTI_SPECIMEN_GENOMIC_FILE(GENOMIC_FILE):
        ID = None

    class READ_GROUP:
        ID = None
        PAIRED_END = None
        FLOW_CELL = None
        LANE_NUMBER = None
        QUALITY_SCALE = None

    class SEQUENCING:
        ID = None
        DATE = None
        STRATEGY = None
        PAIRED_END = None
        LIBRARY_NAME = None
        LIBRARY_STRAND = None
        PLATFORM = None
        INSTRUMENT = None
        INSERT_SIZE = None
        REFERENCE_GENOME = None
        MAX_INSERT_SIZE = None
        MEAN_INSERT_SIZE = None
        MEAN_DEPTH = None
        TOTAL_READS = None
        MEAN_READ_LENGTH = None

        class CENTER:
            NAME = None


def compile_schema():
    """
    "Compile" the concept schema

    Populate every concept class attribute with a string that represents
    a path in the concept class hierarchy to reach that attribute.

    Store all the concept property strings in a set for later reference and
    validation.

    This approach eliminates the need to manually assign concept class
    attributes to a string.
    """

    property_path = []
    property_paths = set()
    _set_cls_attrs(CONCEPT, None, property_path, property_paths)

    return property_paths


def _set_cls_attrs(node, prev_node, property_path, property_paths):
    """
    Recursive method to traverse a class hierarchy and set class attributes
    equal to a string which represents a path in the hierarchy to reach the
    attribute.

    For example, after running the method on this class definition:
        class A:
            class B:
                ID = None
                class C:
                    ID = None
            AGE = None

    The values of the attributes would be equal to this:
        A.AGE = "A.AGE"
        A.B.ID = "A.B.ID"
        A.B.C.ID = "A.B.C.ID"
    """
    # Process a class or child node
    if callable(node):
        # Add class name to property path
        property_path.append(str(node.__name__))
        # Iterate over class attrs
        for attr_name, value in vars(node).items():
            if not attr_name.startswith('__'):
                # Recurse
                if callable(value):
                    _set_cls_attrs(value, node,
                                   property_path,
                                   property_paths)
                else:
                    _set_cls_attrs(attr_name, node,
                                   property_path,
                                   property_paths)
    # Process leaf nodes
    else:
        # Add attribute to property path
        property_path.append(node)
        # Create property string
        property_path_str = DELIMITER.join(property_path)
        # Set attribute on class to equal the property path string
        setattr(prev_node, node, property_path_str)
        # Add property string to list of property path strings
        property_paths.add(property_path_str)

    property_path.pop()


# Set the concept class attributes with their serialized property strings
# Create a set of the serialized concept property strings
concept_property_set = compile_schema()

# Create a set of standard concepts
concept_set = {
    CONCEPT.FAMILY,
    CONCEPT.SEQUENCING,
    CONCEPT.PARTICIPANT,
    CONCEPT.BIOSPECIMEN,
    CONCEPT.DIAGNOSIS,
    CONCEPT.PHENOTYPE,
    CONCEPT.DIAGNOSIS,
    CONCEPT.OUTCOME,
    CONCEPT.GENOMIC_FILE,
    CONCEPT.READ_GROUP
}


def _create_identifiers():
    """
    Build the set of identifying concept properties.

    - All CONCEPT.<>.ID properties are identifiers
    - Some concepts have additional identifying properties
    """
    identifiers = {}
    for concept in concept_set:
        identifiers[concept] = {concept.ID}

    # Add other misc identifiers
    identifiers[CONCEPT.GENOMIC_FILE].add(CONCEPT.GENOMIC_FILE.FILE_PATH)
    identifiers[CONCEPT.SEQUENCING].add(CONCEPT.SEQUENCING.LIBRARY_NAME)
    identifiers[CONCEPT.BIOSPECIMEN].add(CONCEPT.BIOSPECIMEN.ALIQUOT_ID)

    return identifiers


# Create the set of identifier concept properties
identifiers = _create_identifiers()


def is_identifier(concept_property_string):
    """
    Given a delimited concept property string check whether the property
    is an identifying property for this concept.
     :param concept_property_string: a string like: PARTICIPANT|ID
    """

    return concept_property_string in set().union(*identifiers.values())
