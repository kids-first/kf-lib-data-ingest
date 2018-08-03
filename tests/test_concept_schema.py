import pytest

from etl.transform.standard_model.concept_schema import (
    is_identifier,
    validate_concept_string,
    ConceptSchemaValidationError,
    concept_set
)


def test_validate():
    """
    Test concept string validation against the defined
    standard concepts.
    """
    # Invalid concept
    s = 'CHICKEN|ID'
    with pytest.raises(ConceptSchemaValidationError):
        validate_concept_string(s)

    # Invalid concept property
    s = 'PARTICIPANT|NON_EXIST'
    with pytest.raises(ConceptSchemaValidationError):
        validate_concept_string(s)

    # Valid concept property with whitespace
    s = '  PARTICIPANT| ID  '
    assert validate_concept_string(s)

    # Case insensitivity
    s = 'participant|ID'
    assert validate_concept_string(s)


def test_identifiers():
    """
    Test identifier concept properties
    """
    # Is not an identifier a property
    s = 'CONCEPT|PARTICIPANT|IS_PROBAND'
    assert not is_identifier(s)

    # Is not valid concept
    s = 'CHICKEN|ID'
    with pytest.raises(ConceptSchemaValidationError):
        is_identifier(s)

    # Valid concept and property
    for concept in concept_set:
        assert is_identifier('{}|ID'.format(concept.__name__))
