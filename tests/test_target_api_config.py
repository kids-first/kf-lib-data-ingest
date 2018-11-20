import os
import random

import pytest

from conftest import TEST_ROOT_DIR

from kf_lib_data_ingest.etl.configuration.target_api_config import TargetAPIConfig
from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import CONCEPT
KIDS_FIRST_CONFIG = os.path.join(os.path.dirname(TEST_ROOT_DIR),
                                 'kf_lib_data_ingest',
                                 'target_apis', 'kids_first.py')


@pytest.fixture(scope='function')
def kids_first_config():
    """
    Use the kids_first.py target api config module for all of the tests

    This fixture is also testing the kids_first.py config by virtue of the fact
    that no exceptions should be raised when a TargetAPIConfig obj
    is instantiated.

    The config will be modified by each test
    """
    return TargetAPIConfig(KIDS_FIRST_CONFIG)


@pytest.mark.parametrize('test_attr',
                         [('target_concepts'),
                          ('relationships'),
                          ('target_service_entity_id')
                          ])
def test_missing_req_attrs(kids_first_config, test_attr):
    """
    Test for missing required module attributes in a target API config
    """
    # Delete the required module attribute under test
    delattr(kids_first_config.contents, test_attr)

    with pytest.raises(AttributeError) as e:
        kids_first_config._validate_required_attrs()
        assert 'Missing one of the required keys' in str(e)


@pytest.mark.parametrize('test_attr,test_value',
                         [('target_concepts', 'foo'),
                          ('relationships', 'foo'),
                          ('target_service_entity_id', {})
                          ])
def test_req_attr_wrong_type(kids_first_config, test_attr, test_value):
    """
    Test for wrong type of required attributes in a target api config
    """
    # Modify the attribute under test
    setattr(kids_first_config.contents, test_attr, test_value)

    # Test
    with pytest.raises(TypeError):
        kids_first_config._validate_required_attrs()


@pytest.mark.parametrize('test_key',
                         [('standard_concept'),
                          ('properties'),
                          ('endpoint')
                          ])
def test_missing_req_target_concept_keys(kids_first_config, test_key):
    """
    Test for missing required keys in target concept dict
    """
    target_concepts = kids_first_config.contents.target_concepts

    # Delete a req key
    target_concepts['study'].pop(test_key, None)

    # Test
    with pytest.raises(KeyError) as e:
        kids_first_config._validate_required_keys()
        assert test_key in str(e)
        assert 'Missing one of target concept dict required keys' in str(e)


def test_mapped_standard_concepts(kids_first_config):
    """
    Test for invalid standard_concept mappings. Invalid = not a standard
    concept
    """
    # Pick a random concept, modify a mapped standard concept
    target_concepts = kids_first_config.contents.target_concepts
    target_concept = random.choice(list(target_concepts.keys()))

    # Invalid mapping
    mapped_value = 'foo'
    target_concepts[target_concept]['standard_concept'] = mapped_value

    # Test
    with pytest.raises(ValueError) as e:
        kids_first_config._validate_mapped_standard_concepts()
        assert target_concept in str(e)
        assert f'The mapped standard concept: {mapped_value}' in str(e)


def test_target_concept_attr(kids_first_config):
    """
    Test for invalid mapped target concept attribute.
    Invalid = not a standard concept attribute
    """
    # Pick a random target concept and attr
    target_concepts = kids_first_config.contents.target_concepts
    target_concept = random.choice(list(target_concepts.keys()))
    properties = target_concepts[target_concept]['properties']
    attr = random.choice(list(properties.keys()))

    # Invalid mapping
    mapped_value = 'foo'
    properties[attr] = mapped_value

    # Test
    with pytest.raises(ValueError) as e:
        kids_first_config._validate_target_concept_attr_mappings()
        assert mapped_value in str(e)
        assert f'{target_concept}.{attr}' in str(e)


def test_mapped_target_concept_attr(kids_first_config):
    """
    Test for non-string type of a target concept attribute
    """
    # Pick a random target concept
    target_concepts = kids_first_config.contents.target_concepts
    target_concept = random.choice(list(target_concepts.keys()))
    properties = target_concepts[target_concept]['properties']

    # Inject a non-string target concept attribute
    properties[5] = CONCEPT.STUDY.ID

    # Test
    with pytest.raises(TypeError) as e:
        kids_first_config._validate_target_concept_attr_mappings()
        assert 'All target concept attributes must be strings' in str(e)


def test_endpoints(kids_first_config):
    """
    Test for non-string type endpoints
    """
    # Pick a random target concept
    target_concepts = kids_first_config.contents.target_concepts
    target_concept = random.choice(list(target_concepts.keys()))

    # Modify value under test
    target_concepts[target_concept]['endpoint'] = 5

    # Test
    with pytest.raises(TypeError) as e:
        kids_first_config._validate_endpoints()
        assert 'All values in "endpoints" dict must be strings' in str(e)


def test_relationships_types_and_values(kids_first_config):
    """
    Test for wrong types and values in relationships graph
    """
    relationships = kids_first_config.contents.relationships

    # Test non-set value
    parent_concept = random.choice(list(relationships.keys()))
    _saved = relationships[parent_concept]

    relationships[parent_concept] = 'bar'
    with pytest.raises(TypeError) as e:
        kids_first_config._validate_relationships()
        assert 'All values in relationships dict must be sets.' in str(e)
    # Reset
    relationships[parent_concept] = _saved

    # Test non-standard_concept key
    relationships['foo'] = {'bar'}
    with pytest.raises(ValueError) as e:
        kids_first_config._validate_relationships()
        assert ('Keys in relationships dict must be '
                'one of the standard concepts') in str(e)
    # Remove invalid key
    relationships.pop('foo')

    # Test non-standard_concept set member
    relationships[parent_concept] = {'baz'}
    with pytest.raises(ValueError) as e:
        kids_first_config._validate_relationships()
        assert ('Set values in relationships dict must be one of the standard '
                'concepts.') in str(e)


def test_relationships_dag(kids_first_config):
    """
    Test that relationships graph must be a directed acyclic graph
    """
    # Family -> Participant already exists
    relationships = kids_first_config.contents.relationships
    assert relationships[CONCEPT.FAMILY] == {CONCEPT.PARTICIPANT}

    # Now add a cycle Participant -> Biospecimen -> Family
    relationships[CONCEPT.BIOSPECIMEN] = {CONCEPT.FAMILY}

    with pytest.raises(ValueError) as e:
        kids_first_config._validate_relationships()
        assert 'MUST be a directed acyclic graph' in str(e)
