from kf_lib_data_ingest.common.misc import obj_attrs_to_dict

DELIMITER = "|"
UNIQUE_ID_ATTR = "UNIQUE_KEY"


class FileMixin(object):
    SIZE = None
    FILE_NAME = None
    HASH_DICT = None
    URL_LIST = None
    ACL = None
    AVAILABILITY = None
    CONTROLLED_ACCESS = None
    FILE_FORMAT = None
    DATA_TYPE = None


class PropertyMixin(object):
    _CONCEPT_NAME = None
    UNIQUE_KEY = None
    ID = None
    TARGET_SERVICE_ID = None
    HIDDEN = None  # obverse of VISIBLE
    VISIBLE = None  # obverse of HIDDEN


class CONCEPT:
    class INVESTIGATOR(PropertyMixin):
        NAME = None
        INSTITUTION = None

    class STUDY(PropertyMixin):
        AUTHORITY = None
        VERSION = None
        NAME = None
        SHORT_NAME = None
        ATTRIBUTION = None
        RELEASE_STATUS = None
        CATEGORY = None

    class STUDY_FILE(PropertyMixin, FileMixin):
        pass

    class FAMILY(PropertyMixin):
        pass

    class FAMILY_RELATIONSHIP(PropertyMixin):
        class PERSON1(PropertyMixin):
            pass

        class PERSON2(PropertyMixin):
            pass

        RELATION_FROM_1_TO_2 = None

    class PARTICIPANT(PropertyMixin):
        IS_PROBAND = None
        FATHER_ID = None
        MOTHER_ID = None
        PROBAND_ID = None
        RELATIONSHIP_TO_PROBAND = None
        GENDER = None
        ETHNICITY = None
        RACE = None
        CONSENT_TYPE = None
        # affected by diagnoses/phenotypes specifically mentioned by the study
        IS_AFFECTED_UNDER_STUDY = None

    class OUTCOME(PropertyMixin):
        VITAL_STATUS = None
        EVENT_AGE_DAYS = None
        DISEASE_RELATED = None

    class DIAGNOSIS(PropertyMixin):
        NAME = None
        TUMOR_LOCATION = None
        SPATIAL_DESCRIPTOR = None
        CATEGORY = None
        UBERON_TUMOR_LOCATION_ID = None
        EVENT_AGE_DAYS = None
        MONDO_ID = None
        NCIT_ID = None
        ICD_ID = None

    class PHENOTYPE(PropertyMixin):
        NAME = None
        HPO_ID = None
        SNOMED_ID = None
        OBSERVED = None
        EVENT_AGE_DAYS = None

    class BIOSPECIMEN_GROUP(PropertyMixin):
        pass

    class BIOSPECIMEN(PropertyMixin):
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

    class GENOMIC_FILE(PropertyMixin, FileMixin):
        HARMONIZED = None
        SOURCE_FILE = None
        REFERENCE_GENOME = None

    class READ_GROUP(PropertyMixin):
        PAIRED_END = None
        FLOW_CELL = None
        LANE_NUMBER = None
        QUALITY_SCALE = None

    class SEQUENCING(PropertyMixin):
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

        class CENTER(PropertyMixin):
            NAME = None

    class BIOSPECIMEN_GENOMIC_FILE(PropertyMixin):
        pass

    class SEQUENCING_GENOMIC_FILE(PropertyMixin):
        pass

    class BIOSPECIMEN_DIAGNOSIS(PropertyMixin):
        pass

    class READ_GROUP_GENOMIC_FILE(PropertyMixin):
        pass


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
    _set_cls_attrs(
        CONCEPT, None, property_path, property_paths, include_root=False
    )
    return property_paths


str_to_CONCEPT = {}


def _set_cls_attrs(
    node, prev_node, property_path, property_paths, include_root=False
):
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

    Given a delimiter set to '|', the values of the attributes would be:
        A.AGE = "A|AGE"
        A.B.ID = "A|B|ID"
        A.B.C.ID = "A|B|C|ID"
    """
    # Process a class or child node
    if callable(node):
        # Add class name to property path
        property_path.append(str(node.__name__))
        # Iterate over class attrs
        for attr_name, value in obj_attrs_to_dict(node).items():
            # Recurse
            if callable(value):
                _set_cls_attrs(
                    value,
                    node,
                    property_path,
                    property_paths,
                    include_root=include_root,
                )
            else:
                _set_cls_attrs(
                    attr_name,
                    node,
                    property_path,
                    property_paths,
                    include_root=include_root,
                )
    # Process leaf nodes
    else:
        # Don't include root in property path
        if not include_root:
            property_path = property_path[1:]
        # Create current path str
        concept_name_str = DELIMITER.join(property_path)
        # Add attribute to property path
        property_path.append(node)
        # Create property string
        property_path_str = DELIMITER.join(property_path)
        # Set attribute on class to equal the property path string OR
        # The concept name path string if the attribute is _CONCEPT_NAME
        if node == "_CONCEPT_NAME":
            setattr(prev_node, node, concept_name_str)
            str_to_CONCEPT[concept_name_str] = prev_node
        else:
            setattr(prev_node, node, property_path_str)

        # Add property string to list of property path strings
        property_paths.add(property_path_str)

    property_path.pop()


# Set the concept class attributes with their serialized property strings
# Create a set of the serialized concept property strings
concept_property_set = compile_schema()

# Create a set of standard concepts
concept_set = {
    CONCEPT.STUDY,
    CONCEPT.INVESTIGATOR,
    CONCEPT.FAMILY,
    CONCEPT.SEQUENCING,
    CONCEPT.SEQUENCING.CENTER,
    CONCEPT.PARTICIPANT,
    CONCEPT.FAMILY_RELATIONSHIP,
    CONCEPT.FAMILY_RELATIONSHIP.PERSON1,
    CONCEPT.FAMILY_RELATIONSHIP.PERSON2,
    CONCEPT.BIOSPECIMEN_GROUP,
    CONCEPT.BIOSPECIMEN,
    CONCEPT.DIAGNOSIS,
    CONCEPT.PHENOTYPE,
    CONCEPT.DIAGNOSIS,
    CONCEPT.OUTCOME,
    CONCEPT.GENOMIC_FILE,
    CONCEPT.READ_GROUP,
    CONCEPT.BIOSPECIMEN_GENOMIC_FILE,
    CONCEPT.BIOSPECIMEN_DIAGNOSIS,
    CONCEPT.READ_GROUP_GENOMIC_FILE,
    CONCEPT.SEQUENCING_GENOMIC_FILE,
}


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


def is_identifier(concept_property_string):
    """
    Given a delimited concept property string, check whether it is a
    UNIQUE_KEY property. The UNIQUE_KEY property is a standard concept property
    which is reserved to uniquely identify concept instances of the same type.

    :param concept_property_string: a concept attribute string such as
    CONCEPT|PARTICIPANT|ID
    """

    return concept_attr_from(concept_property_string) == UNIQUE_ID_ATTR


concept_property_split = {}
for conprop in concept_property_set:
    con, prop = conprop.rsplit(DELIMITER, 1)
    concept_property_split[conprop] = (con, prop)


def concept_from(concept_attribute_str):
    """
    Extract the concept from the concept attribute string
    """
    return concept_property_split[concept_attribute_str][0]


def concept_attr_from(concept_attribute_str):
    """
    Extract the concept attribute from the concept attribute string
    """
    return concept_property_split[concept_attribute_str][1]


unique_key_composition = _create_unique_key_composition()
