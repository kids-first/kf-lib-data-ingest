import pytest

from kf_lib_data_ingest.etl.transform.standard_model.graph import (
    ConceptNode,
    ConceptGraph
)
from kf_lib_data_ingest.etl.transform.standard_model.concept_schema import (
    CONCEPT,
    DELIMITER
)


@pytest.fixture
def concept_graph():
    return ConceptGraph()


def test_construct_node():
    # Construct identifier node
    concept_prop_str = CONCEPT.PARTICIPANT.ID
    value = 'P1'
    node = ConceptNode(concept_prop_str, value)

    # Check node key
    assert node.key == concept_prop_str + '|{}'.format(value)
    # Check node identifier
    assert node.is_identifier
    # Check uid
    assert node.uid is None

    # Construct non-identifier node
    concept_prop_str = CONCEPT.PARTICIPANT.IS_PROBAND
    value = False
    node = ConceptNode(concept_prop_str, value)

    # Check node identifier
    assert not node.is_identifier


def test_node_uid():
    concept_prop_str = CONCEPT.PARTICIPANT.ID
    value = 'P1'
    extract_config_url = '/extract_config_1.py'
    source_file_url = '/datafile.csv'
    r = 0
    c = 0
    node = ConceptNode(concept_prop_str, value,
                       extract_config_url=extract_config_url,
                       source_file_url=source_file_url,
                       row=r,
                       col=c)
    # Check uid
    assert node.uid == DELIMITER.join([extract_config_url,
                                       source_file_url,
                                       str(r),
                                       str(c)])


def test_add_and_get_node(concept_graph):
    """
    Test adding and getting nodes from ConceptGraph
    """
    # Add attribute node
    concept_prop_str = CONCEPT.PARTICIPANT.IS_PROBAND
    value = False
    node = concept_graph.add_or_get_node(concept_prop_str, value)

    # Check attribute node is in graph
    assert node.key in concept_graph.graph.nodes

    # Check attribute node is not in concept index since its not an
    # identifier
    concept_instances = concept_graph.id_index.get(node.concept)
    assert not concept_instances

    # Check attribute node is in attribute index
    assert node.concept_attribute_pair in concept_graph.attribute_index
    assert node in (
        concept_graph.attribute_index[node.concept_attribute_pair])

    # Test get attribute node
    assert concept_graph.get_node(node.key) == node

    # Add identifier node
    concept_prop_str = CONCEPT.PARTICIPANT.ID
    value = 'P1'
    node = concept_graph.add_or_get_node(concept_prop_str, value)

    # Check node is in concept index since it is an identifier
    concept_instances = concept_graph.id_index.get(node.concept)
    assert node.key in concept_instances
    # And not in attribute index
    assert node.concept_attribute_pair not in concept_graph.attribute_index
    # The stored value is a ConceptNode
    assert isinstance(concept_instances.get(node.key), ConceptNode)

    # Test that you cannot add a node if it already exists
    result = concept_graph.add_or_get_node(concept_prop_str, value,
                                           row=5, col=5)
    assert result.row != 5
    assert result.col != 5


def test_add_edge_and_exists(concept_graph):
    """
    Test add un/directed edges on ConceptGraph
    """
    # Test directed edge
    node1 = ConceptNode(CONCEPT.BIOSPECIMEN.TISSUE_TYPE, 'normal')
    node2 = ConceptNode(CONCEPT.BIOSPECIMEN.ID, 'B1')
    concept_graph._add_edge(node2, node1)

    # Check nodes exist
    assert node1 == concept_graph.get_node(node1.key)
    assert node2 == concept_graph.get_node(node2.key)

    # Manually check networkx, does edge exist and is it only one way
    assert concept_graph.graph.has_edge(node2.key, node1.key)
    assert not concept_graph.graph.has_edge(node1.key, node2.key)

    # Test _edge_exists method
    assert concept_graph._edge_exists(node2, node1, bidirectional=False)
    assert not concept_graph._edge_exists(node2, node1, bidirectional=True)

    # Test undirected edge
    concept_graph._add_edge(node1, node2)
    assert concept_graph.graph.has_edge(node1.key, node2.key)

    # Test _edge_exists method
    assert concept_graph._edge_exists(node1, node2, bidirectional=True)


def test_connect_id_nodes(concept_graph):
    """
    Test connect_id_nodes and connect_attribute_node
    """

    # Create id nodes
    p1 = concept_graph.add_or_get_node(CONCEPT.PARTICIPANT.ID, 'P1')
    b1 = concept_graph.add_or_get_node(CONCEPT.BIOSPECIMEN.ID, 'B1')
    g1 = concept_graph.add_or_get_node(CONCEPT.GENOMIC_FILE.ID, 'GF1')
    id_nodes = [p1, b1, g1]

    # Connect id nodes
    concept_graph.connect_id_nodes(id_nodes)

    # Check if edges exist
    assert concept_graph.graph.size() == 4
    assert concept_graph._edge_exists(p1, b1, bidirectional=True)
    assert concept_graph._edge_exists(b1, g1, bidirectional=True)

    # Test that you cannot connect two id nodes if a path already exists
    concept_graph.connect_id_nodes([p1, g1])
    assert concept_graph.graph.size() == 4
    assert not concept_graph._edge_exists(p1, g1, bidirectional=True)


def test_connect_attribute_nodes(concept_graph):
    """
    Test connect_id_nodes and connect_attribute_node
    """
    # Create attribute node
    tissue_type = concept_graph.add_or_get_node(
        CONCEPT.BIOSPECIMEN.TISSUE_TYPE, 'Normal')

    # Create id nodes
    p1 = concept_graph.add_or_get_node(CONCEPT.PARTICIPANT.ID, 'P1')
    b1 = concept_graph.add_or_get_node(CONCEPT.BIOSPECIMEN.ID, 'B1')
    id_nodes = [p1, b1]

    # Connect attribute nodes
    concept_graph.connect_attribute_node(tissue_type, id_nodes)

    # Test that tissue type has no outgoing edges and 2 incoming edges
    assert concept_graph.graph.out_degree(tissue_type.key) == 0
    assert concept_graph.graph.in_degree(tissue_type.key) == 2
    for node in id_nodes:
        assert concept_graph._edge_exists(node, tissue_type)
