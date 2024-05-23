from kf_lib_data_ingest.common.misc import obj_attrs_to_dict

DELIMITER = "|"
UNIQUE_ID_ATTR = "UNIQUE_KEY"


class QuantityMixin(object):
    VALUE = None
    UNITS = None


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
    VISIBILTIY_REASON = None
    VISIBILITY_COMMENT = None


class CONCEPT:
    class PROJECT:
        ID = None

    class INVESTIGATOR(PropertyMixin):
        NAME = None
        INSTITUTION = None

    class STUDY(PropertyMixin):
        AUTHORITY = None
        VERSION = None
        NAME = None
        SHORT_NAME = None
        ATTRIBUTION = None
        FILE_VERSION_DESCRIPTOR = None
        CATEGORY = None

    class STUDY_FILE(PropertyMixin, FileMixin):
        pass

    class FAMILY(PropertyMixin):
        pass

    class FAMILY_RELATIONSHIP(PropertyMixin):
        class PERSON1(PropertyMixin):
            GENDER = None
            pass

        class PERSON2(PropertyMixin):
            GENDER = None
            pass

        RELATION_FROM_1_TO_2 = None

    class PARTICIPANT(PropertyMixin):
        IS_PROBAND = None
        FATHER_ID = None
        MOTHER_ID = None
        PROBAND_ID = None
        RELATIONSHIP_TO_PROBAND = None
        GENDER = None
        SEX = None
        ETHNICITY = None
        RACE = None
        CONSENT_TYPE = None
        # affected by diagnoses/phenotypes specifically mentioned by the study
        IS_AFFECTED_UNDER_STUDY = None
        SPECIES = None
        ENROLLMENT_AGE_DAYS = None
        LAST_CONTACT_AGE_DAYS = None

        class ENROLLMENT_AGE(QuantityMixin):
            pass

        class LAST_CONTACT_AGE(QuantityMixin):
            pass

    class OUTCOME(PropertyMixin):
        VITAL_STATUS = None
        EVENT_AGE_DAYS = None

        class EVENT_AGE(QuantityMixin):
            pass

        DISEASE_RELATED = None

    class DIAGNOSIS(PropertyMixin):
        NAME = None
        TUMOR_LOCATION = None
        SPATIAL_DESCRIPTOR = None
        CATEGORY = None
        UBERON_TUMOR_LOCATION_ID = None
        EVENT_AGE_DAYS = None
        VERIFICATION = None

        class ABATEMENT_EVENT_AGE(QuantityMixin):
            pass

        class EVENT_AGE(QuantityMixin):
            pass

        MONDO_ID = None
        NCIT_ID = None
        ICD_ID = None

    class PHENOTYPE(PropertyMixin):
        NAME = None
        HPO_ID = None
        SNOMED_ID = None
        OBSERVED = None
        EVENT_AGE_DAYS = None
        INTERPRETATION = None
        VERIFICATION = None

        class ABATEMENT_EVENT_AGE(QuantityMixin):
            pass

        class EVENT_AGE(QuantityMixin):
            pass

    class OBSERVATION(PropertyMixin):
        NAME = None
        ONTOLOGY_ONTOBEE_URI = None
        ONTOLOGY_CODE = None
        CATEGORY = None
        INTERPRETATION = None
        STATUS = None
        ANATOMY_SITE = None
        UBERON_ANATOMY_SITE_ID = None

        class EVENT_AGE(QuantityMixin):
            pass

    class BIOSPECIMEN_GROUP(PropertyMixin):
        pass

    class SAMPLE(PropertyMixin):
        TISSUE_TYPE = None
        NCIT_TISSUE_TYPE_ID = None
        ANATOMY_SITE = None
        NCIT_ANATOMY_SITE_ID = None
        UBERON_ANATOMY_SITE_ID = None
        COMPOSITION = None
        TUMOR_DESCRIPTOR = None
        EVENT_ID = None
        EVENT_AGE_DAYS = None

        class EVENT_AGE(QuantityMixin):
            pass

        class VOLUME(QuantityMixin):
            pass

        SPATIAL_DESCRIPTOR = None
        SHIPMENT_ORIGIN = None
        SHIPMENT_DATE = None
        VOLUME_UL = None
        SAMPLE_PROCUREMENT = None
        PRESERVATION_METHOD = None

    class BIOSPECIMEN(SAMPLE):
        class QUANTITY(QuantityMixin):
            pass

        class CONCENTRATION(QuantityMixin):
            pass

        ANALYTE = None
        CONCENTRATION_MG_PER_ML = None
        DBGAP_STYLE_CONSENT_CODE = None
        CONSENT_SHORT_NAME = None

    class GENOMIC_FILE(PropertyMixin, FileMixin):
        HARMONIZED = None
        SOURCE_FILE = None
        REFERENCE_GENOME = None
        WORKFLOW_TOOL = None
        WORKFLOW_TYPE = None
        WORKFLOW_VERSION = None
        FILE_VERSION_DESCRIPTOR = None
        DATA_CATEGORY = None

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
        LIBRARY_SELECTION = None
        LIBRARY_PREP = None
        PLATFORM = None
        INSTRUMENT = None
        INSERT_SIZE = None
        REFERENCE_GENOME = None
        MAX_INSERT_SIZE = None
        MEAN_INSERT_SIZE = None
        MEAN_DEPTH = None
        TOTAL_READS = None
        MEAN_READ_LENGTH = None
        TARGET_CAPTURE_KIT = None
        IS_ADAPTER_TRIMMED = None
        ADAPTER_SEQUENCING = None
        READ_PAIR_NUMBER = None

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
    CONCEPT.SAMPLE,
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
