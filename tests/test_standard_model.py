import os

import pytest
import pandas as pd

from kf_lib_data_ingest.common.constants import *
from kf_lib_data_ingest.common.concept_schema import (
    CONCEPT,
    DELIMITER,
    UNIQUE_ID_ATTR,
    concept_from,
    is_identifier,
    unique_key_composition
)
from kf_lib_data_ingest.etl.transform.common import insert_unique_keys
from kf_lib_data_ingest.etl.transform.standard_model.model import StandardModel
from kf_lib_data_ingest.etl.transform.standard_model.graph import (
    ConceptNode,
    export_to_gml,
    import_from_gml
)


@pytest.fixture(scope='function')
def data_df_dict(transform_stage):
    """
    Create an input data dict to populate the standard model.

    This data is somewhat simple so that we can easily visualize it and test
    specific parts of standard_model.graph.find_attribute_value and
    standard_model.populate.

    TODO - include a more complex multi-table dataset to more thoroughly test
    """
    data = {'pedigree': [{CONCEPT.FAMILY.ID: 'F1',
                          CONCEPT.PARTICIPANT.ID: 'P1',
                          CONCEPT.PARTICIPANT.RACE: RACE.ASIAN},
                         {CONCEPT.FAMILY.ID: 'F1',
                          CONCEPT.PARTICIPANT.ID: 'P2',
                          CONCEPT.PARTICIPANT.RACE: None}],
            'subject_sample': [{CONCEPT.PARTICIPANT.ID: 'P1',
                                CONCEPT.BIOSPECIMEN.ID: 'B1'},
                               {CONCEPT.PARTICIPANT.ID: 'P1',
                                CONCEPT.BIOSPECIMEN.ID: 'B2'},
                               {CONCEPT.PARTICIPANT.ID: 'P2',
                                CONCEPT.BIOSPECIMEN.ID: 'B3'}],
            'sample': [{CONCEPT.BIOSPECIMEN.ID: 'B1',
                        CONCEPT.PARTICIPANT.RACE: None},
                       {CONCEPT.BIOSPECIMEN.ID: 'B2',
                        CONCEPT.PARTICIPANT.RACE: None},
                       {CONCEPT.BIOSPECIMEN.ID: 'B3',
                        CONCEPT.PARTICIPANT.RACE: RACE.WHITE}]}
    df_dict = {f's3://bucket/key/{k}.csv':
               (f'file:///study/configs/{k}.py', pd.DataFrame(v))
               for k, v in data.items()}

    insert_unique_keys(df_dict)

    return df_dict


@pytest.fixture(scope='function')
def model(data_df_dict):
    """
    Create and populate the standard model
    """
    # Standard model
    model = StandardModel()
    model.populate(data_df_dict)

    # Export concept graph to gml file
    from kf_lib_data_ingest.etl.transform.standard_model.graph import (
        export_to_gml
    )
    from conftest import TEST_DATA_DIR

    export_to_gml(model.concept_graph, os.path.join(TEST_DATA_DIR,
                                                    'test_graph.gml'))

    return model


@pytest.fixture(scope='function')
def random_data_df_dict():
    """
    Create composite table containing random participant, family, biospecimen
    data appropriately formatted for populating standard concept model.

    This is supposed to be more complex than data_df_dict and is meant for
    general testing of standard_model.graph.find_attribute_value

    TODO - make this more complex data later
    """
    import random
    n_participants = 10
    family_id_pool = [f'F{i}' for i in range(n_participants // 3)]
    races = [RACE.WHITE, RACE.ASIAN, RACE.BLACK]
    compositions = [SPECIMEN.COMPOSITION.TISSUE, SPECIMEN.COMPOSITION.BLOOD]
    data_dict = {
        CONCEPT.FAMILY.UNIQUE_KEY: [random.choice(family_id_pool)
                                    for i in range(n_participants)],
        CONCEPT.PARTICIPANT.UNIQUE_KEY: [f'P{i}'
                                         for i in range(n_participants)],
        CONCEPT.BIOSPECIMEN.UNIQUE_KEY: [f'B{i}'
                                         for i in range(n_participants)],
        CONCEPT.PARTICIPANT.RACE: [random.choice(races)
                                   for i in range(n_participants)],
        CONCEPT.BIOSPECIMEN.COMPOSITION: [random.choice(compositions)
                                          for i in range(n_participants)]}
    clinical = pd.DataFrame(data_dict)
    df_dict = {f's3://bucket/key/clinical.csv':
               (f'file:///study/configs/clinical.py', clinical)}

    insert_unique_keys(df_dict)

    return df_dict


@pytest.fixture(scope='function')
def random_model(random_data_df_dict):
    """
    Create and populate the standard model with some random data
    """
    # Standard model
    model = StandardModel()
    model.populate(random_data_df_dict)

    return model


def test_populate_model(data_df_dict):
    """
    Test populating the concept graph in the standard model
    """
    model = StandardModel()
    model.populate(data_df_dict)

    cg = model.concept_graph

    # Check graph
    for extract_config_url, (source_file_url, df) in data_df_dict.items():
        for c, col in enumerate(df.columns):
            for r, value in enumerate(df[col]):
                key = f'{col}|{value}'
                node = cg.get_node(key)
                if value is None:
                    assert not node
                    continue
                # Node exists
                assert node
                # Node has non null value
                assert pd.notnull(node.value)
                # Node has a uid
                assert node.uid
                # Pedigree file was inserted into graph first so uid of
                # participant nodes should contain pointers to participants
                # from the pedigree file
                if node.key.startswith(CONCEPT.PARTICIPANT.UNIQUE_KEY):
                    assert 'pedigree' in node.uid
                # Subject sample file was inserted into graph second so uid of
                # biospecimen nodes should contain pointers to biospecimens
                # from the subject sample file
                elif node.key.startswith(CONCEPT.BIOSPECIMEN.UNIQUE_KEY):
                    assert 'subject_sample' in node.uid


def test_transform_all(target_api_config, random_model):
    """
    Test transformation when a list of target_concepts isn't supplied
    """
    # Test transform all concepts
    # Output should only include the concepts for which there is data
    data = random_model.transform(target_api_config,
                                  target_concepts_to_transform=None)
    all_target_concepts = target_api_config.concept_schemas.keys()
    target_concepts_w_data = ['family', 'participant', 'biospecimen']

    for target_concept, output in data.items():
        if target_concept in target_concepts_w_data:
            assert len(output) > 0
        else:
            assert len(output) == 0

    # All target concepts should be in output
    assert len(all_target_concepts) == len(data.keys())


def test_transform(target_api_config, random_model):
    """
    Test transformation from the standard model to a target model
    """
    # Transform to target model
    target_concepts = ['family', 'participant', 'biospecimen', 'mammals',
                       'phenotype']
    data = random_model.transform(target_api_config, target_concepts)

    # Output should only contain instances of valid target concept types
    # (the ones we supplied that exist in target api config)
    diff = set(target_concepts).symmetric_difference(data.keys())
    assert (len(diff) == 1) and ('mammals' in diff)

    # Number of output instances for a particular concept should match number
    # of id nodes for that concept in the concept graph
    for target_concept, instances in data.items():
        # Get standard concept this target concept maps to
        schema = target_api_config.concept_schemas[target_concept]
        standard_concept = schema['standard_concept']._CONCEPT_NAME

        # Check counts
        expected_count = 0
        nodes = random_model.concept_graph.id_index.get(standard_concept)
        if nodes:
            expected_count = len(nodes)
        assert expected_count == len(instances)

        # Check content
        for instance in instances:
            assert instance['id']
            if target_concept == 'participant':
                assert instance['properties']['race'] is not None
            if target_concept == 'biospecimen':
                assert instance['properties']['composition'] is not None

    # There should be 0 phenotypes since we have no phenotype data
    assert 0 == len(data['phenotype'])


@pytest.mark.parametrize('node_concept,neighbor_key,expected_output',
                         [
                             # Study, Family F1
                             (CONCEPT.STUDY._CONCEPT_NAME,
                              f'{CONCEPT.FAMILY.UNIQUE_KEY}{DELIMITER}F1',
                              True),

                             # Participant, Family F1
                             (CONCEPT.PARTICIPANT._CONCEPT_NAME,
                              f'{CONCEPT.FAMILY.UNIQUE_KEY}{DELIMITER}F1',
                              False),

                             # Participant, Biospecimen B1
                             (CONCEPT.PARTICIPANT._CONCEPT_NAME,
                              f'{CONCEPT.BIOSPECIMEN.UNIQUE_KEY}{DELIMITER}B1',
                              True),

                             # Participant, Biospecimen B2
                             (CONCEPT.PARTICIPANT._CONCEPT_NAME,
                              f'{CONCEPT.BIOSPECIMEN.UNIQUE_KEY}{DELIMITER}B1',
                              True)

                         ]
                         )
def test_is_neighbor_valid(target_api_config, model, node_concept,
                           neighbor_key, expected_output):
    """
    Test ConceptGraph._is_neighbor_valid

    When searching for a CONCEPT|A attribute, is CONCEPT|B|ID|B1
    a valid neighbor to traverse? It should be if:
        1. CONCEPT|B is not an ancestor of CONCEPT|A OR
        2. CONCEPT|B is an ancestor of CONCEPT|A and CONCEPT|B is not connected
         to any other CONCEPT|B nodes or nodes with a concept that is a
          descendant of CONCEPT|B.
    """
    g = model.concept_graph
    rg = target_api_config.relationship_graph

    # Test is neighbor is valid
    neighbor = g.get_node(neighbor_key)
    assert g._is_neighbor_valid(node_concept, neighbor, rg) == expected_output


@pytest.mark.parametrize('id_node_key,concept_attr,expected_output',
                         [
                             # Participant P1, Race
                             (f'{CONCEPT.PARTICIPANT.UNIQUE_KEY}{DELIMITER}P1',
                              CONCEPT.PARTICIPANT.RACE, RACE.ASIAN),

                             # Participant P2, Race
                             (f'{CONCEPT.PARTICIPANT.UNIQUE_KEY}{DELIMITER}P2',
                                 CONCEPT.PARTICIPANT.RACE, RACE.WHITE),

                             # Biospecimen B1, Tissue type
                             (f'{CONCEPT.BIOSPECIMEN.UNIQUE_KEY}{DELIMITER}B1',
                                 CONCEPT.BIOSPECIMEN.TISSUE_TYPE, None)
                         ]
                         )
def test_find_attribute_value(target_api_config, model, id_node_key,
                              concept_attr, expected_output):

    g = model.concept_graph
    rg = target_api_config.relationship_graph

    # Test find concept attribute value
    start_node = g.get_node(id_node_key)
    assert g.find_attribute_value(start_node,
                                  concept_attr,
                                  rg) == expected_output


def test_find_non_existent(target_api_config, model):
    """
    Test that find for a start node that does not exist in the concept graph

    ... this should theoretically never happen since the calling code can
    never pass in a non-existent start node. But testing for completeness...
    """
    # Create a non-existent node
    start_node = ConceptNode(CONCEPT.PARTICIPANT.UNIQUE_KEY, 'PH1')
    concept_attr = CONCEPT.PHENOTYPE.NAME

    g = model.concept_graph
    rg = target_api_config.relationship_graph

    # Test find concept attribute value
    with pytest.raises(ValueError) as e:
        g.find_attribute_value(start_node, concept_attr, rg)
        assert start_node.key in e


def test_general_find(target_api_config, random_data_df_dict):
    """
    For each concept in random_data_df_dict, for each attribute of the concept,
    verify that the correct value for the attribute is found in the
    concept graph.
    """
    # Populate standard model with random_data_df_dict data
    model = StandardModel()
    model.populate(random_data_df_dict)
    g = model.concept_graph
    rg = target_api_config.relationship_graph

    df = [tup[1] for tup in random_data_df_dict.values()][0]

    # Run tests
    concepts = {concept_from(col) for col in df.columns}
    links = {
        CONCEPT.PARTICIPANT._CONCEPT_NAME: {CONCEPT.FAMILY.UNIQUE_KEY},
        CONCEPT.BIOSPECIMEN._CONCEPT_NAME: {CONCEPT.PARTICIPANT.UNIQUE_KEY}
    }
    # For each concept in source table
    for concept in concepts:
        # Get the attribute columns for the concept
        attr_cols = [col for col in df.columns
                     if not is_identifier(col) and col.startswith(concept)]
        # For each row in the source table
        for _, row in df.iterrows():
            # Get the node from the graph pertaining to the concept ID
            id_col = f'{concept}{DELIMITER}UNIQUE_KEY'
            _id = row[id_col]
            node = g.get_node(f'{id_col}{DELIMITER}{_id}')
            # For each attribute of the concept instance, verify the value
            # found from the graph matches the value in the source data
            for attr in attr_cols:
                value = g.find_attribute_value(node, attr, rg)
                assert value == row[attr]
            for link in links.get(concept, []):
                value = g.find_attribute_value(node, link, rg)
                assert value == row[link]


def test_gml_import_export(tmpdir_factory, model):
    """
    Test etl.transform.standard_model.graph.export_to_gml and
    etl.transform.standard_model.graph.import_from_gml
    """
    fn = tmpdir_factory.mktemp("data").join("graph.gml")
    export_to_gml(model.concept_graph, str(fn))

    assert os.path.isfile(fn)

    imported_graph = import_from_gml(str(fn))

    for node_key in model.concept_graph.graph.nodes:
        orig_node = model.concept_graph.graph.node.get(node_key)
        node = imported_graph.node.get(node_key)
        assert node
        assert (ConceptNode.to_dict(node['object']) ==
                ConceptNode.to_dict(orig_node['object']))
