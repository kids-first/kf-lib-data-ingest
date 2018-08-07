import pytest

from etl.transform.standard_model.concept_schema import (
    is_identifier,
    concept_set
)


def test_identifiers():
    """
    Test identifier concept properties
    """
    # Is not an identifier a property
    s = 'CONCEPT|PARTICIPANT|IS_PROBAND'
    assert not is_identifier(s)

    # Valid concept and property
    for concept in concept_set:
        assert is_identifier('{}|ID'.format(concept.__name__))
