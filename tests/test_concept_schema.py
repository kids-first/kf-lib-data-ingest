from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT,
    DELIMITER,
    _set_cls_attrs,
    concept_property_set,
    concept_set,
    is_identifier,
)


def test_identifiers():
    """
    Test identifier concept properties
    """
    # Is not an identifier a property
    s = ["PARTICIPANT", "IS_PROBAND"]
    s = DELIMITER.join(s)
    assert not is_identifier(s)

    # Valid concept and property
    for concept in concept_set:
        assert is_identifier(f"{concept._CONCEPT_NAME}{DELIMITER}UNIQUE_KEY")
        assert not is_identifier(f"{concept._CONCEPT_NAME}{DELIMITER}ID")


def test_set_cls_attrs():
    """
    Test method _set_cls_attrs
    """

    # Create a class hierarchy
    class Mixin(object):
        COMMON = None

    class A(Mixin):
        ID = None

        def hello():
            print("hey")

        class B(Mixin):
            ID = None

            class C:
                ID = None
                NAME = None

        class D:
            pass

    property_path = []
    properties = set()
    _set_cls_attrs(A, None, property_path, properties, include_root=True)

    # Check that every class attribute is assigned to its appropriate path str
    test_params = {
        A.COMMON: "A" + DELIMITER + "COMMON",
        A.ID: "A" + DELIMITER + "ID",
        A.B.ID: "A" + DELIMITER + "B" + DELIMITER + "ID",
        A.B.COMMON: "A" + DELIMITER + "B" + DELIMITER + "COMMON",
        A.B.C.ID: "A" + DELIMITER + "B" + DELIMITER + "C" + DELIMITER + "ID",
        A.B.C.NAME: "A"
        + DELIMITER
        + "B"
        + DELIMITER
        + "C"
        + DELIMITER
        + "NAME",  # noqa E501
    }
    for cls_attr, property_str in test_params.items():
        assert cls_attr == property_str
        assert property_str in properties

    # No property strings should exist for prefix A|D
    for property_str in properties:
        assert not property_str.startswith("A" + DELIMITER + "D")


def test_compile_schema():
    """
    Reverse check - test that all generated concept property strings
    do actually resolve to an attribute on a concept class
    """
    for prop_string in concept_property_set:
        assert _validate_concept_string(prop_string)


def _validate_concept_string(concept_property_string):
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
            prev = getattr(prev, node)
        except AttributeError:
            raise Exception(
                "{} does not exist in standard concepts".format(
                    concept_property_string
                )
            )
    return True
