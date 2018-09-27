from pprint import pprint

import pytest
import pandas as pd

from common import constants
from etl.transform.standard_model.concept_schema import CONCEPT, DELIMITER
from etl.transform.standard_model.model import StandardModel
from etl.transform.standard_model.graph import ConceptNode


@pytest.fixture(scope='function')
def data_df():
    data = {'pedigree': [{CONCEPT.FAMILY.ID: 'F1',
                          CONCEPT.PARTICIPANT.ID: 'P1'},
                         {CONCEPT.FAMILY.ID: 'F1',
                          CONCEPT.PARTICIPANT.ID: 'P2'}],
            'subject_sample': [{CONCEPT.PARTICIPANT.ID: 'P1',
                                CONCEPT.BIOSPECIMEN.ID: 'B1'},
                               {CONCEPT.PARTICIPANT.ID: 'P1',
                                CONCEPT.BIOSPECIMEN.ID: 'B2'},
                               {CONCEPT.PARTICIPANT.ID: 'P2',
                                CONCEPT.BIOSPECIMEN.ID: 'B3'}],
            'sample': [{CONCEPT.BIOSPECIMEN.ID: 'B1',
                        CONCEPT.PARTICIPANT.RACE: constants.RACE.WHITE},
                       {CONCEPT.BIOSPECIMEN.ID: 'B2',
                        CONCEPT.PARTICIPANT.RACE: constants.RACE.WHITE},
                       {CONCEPT.BIOSPECIMEN.ID: 'B3',
                        CONCEPT.PARTICIPANT.RACE: constants.RACE.WHITE}]}
    df_dict = {f's3://bucket/key/{k}.csv':
               (f'file:///study/configs/{k}.py', pd.DataFrame(v))
               for k, v in data.items()}

    return df_dict


def test_populate_model(data_df):
    # Populate model
    model = StandardModel()
    model.build(data_df)
    cg = model.concept_graph

    # Check graph
    for extract_config_url, (source_file_url, df) in data_df.items():
        for c, col in enumerate(df.columns):
            for r, value in enumerate(df[col]):
                key = f'{col}|{value}'
                node = cg.get_node(key)
                # Node exists
                assert node
                # Node has a uid
                assert node.uid
                # Pedigree file was inserted into graph first so uid of
                # participant nodes should contain pointers to participants
                # from the pedigree file
                if node.key.startswith(CONCEPT.PARTICIPANT.ID):
                    assert 'pedigree' in node.uid
                # Subject sample file was inserted into graph second so uid of
                # biospecimen nodes should contain pointers to biospecimens
                # from the subject sample file
                elif node.key.startswith(CONCEPT.BIOSPECIMEN.ID):
                    assert 'subject_sample' in node.uid


def test_transform(target_api_config, data_df):
    # Standard model
    model = StandardModel()
    model.build(data_df)

    # Transform to target model
    target_concepts = ['family', 'participant', 'biospecimen', 'mammals',
                       'phenotype']
    data = model.transform(target_api_config, target_concepts)

    # Output should only contain instances of valid target concept types
    # (the ones we supplied that exist in target api config)
    diff = set(target_concepts).symmetric_difference(data.keys())
    assert (len(diff) == 1) and ('mammals' in diff)

    # Number of output instances for a particular concept should match number
    # of id nodes for that concept in the concept graph
    for target_concept, instances in data.items():
        # Get standard concept this target concept maps to
        schema = target_api_config.concept_schemas[target_concept]
        standard_concept_id_str = list(schema['id'].values())[0]
        standard_concept = ConceptNode(standard_concept_id_str, '').concept

        # Check counts
        expected_count = 0
        nodes = model.concept_graph.id_index.get(standard_concept)
        if nodes:
            expected_count = len(nodes)
        assert expected_count == len(instances)

    # There should be 0 phenotypes since we have no phenotype data
    assert 0 == len(data['phenotype'])
