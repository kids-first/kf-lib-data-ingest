import pytest

from etl.transform.standard_model.graph import (
    ConceptPropertyNode,
    ConceptPropertyGraph
)
from etl.transform.standard_model.concept_schema import CONCEPT


@pytest.fixture
def property_graph():
    return ConceptPropertyGraph()


def test_construct_node():
    # Construct identifier node
    concept_prop_str = str(CONCEPT.PARTICIPANT.ID)
    value = 'P1'
    node = ConceptPropertyNode(concept_prop_str, value)

    # Check node key
    assert node.key == concept_prop_str + '|{}'.format(value)
    # Check node identifier
    assert node.is_identifier
    # Check uid
    assert node.uid is None

    # Construct non-identifier node
    concept_prop_str = str(CONCEPT.PARTICIPANT.IS_PROBAND)
    value = False
    node = ConceptPropertyNode(concept_prop_str, value)

    # Check node identifier
    assert not node.is_identifier


def test_node_uid():
    concept_prop_str = str(CONCEPT.PARTICIPANT.ID)
    value = 'P1'
    extract_config_url = '/extract_config_1.py'
    source_url = '/datafile.csv'
    r = 0
    c = 0
    node = ConceptPropertyNode(concept_prop_str, value,
                               extract_config_url=extract_config_url,
                               source_url=source_url,
                               row=r,
                               col=c)
    # Check uid
    assert node.uid == '|'.join([extract_config_url,
                                 source_url,
                                 str(r),
                                 str(c)])


def test_add_and_get_node(property_graph):
    """
    Test adding and getting nodes from ConceptPropertyGraph
    """
    # Add property node
    concept_prop_str = str(CONCEPT.PARTICIPANT.IS_PROBAND)
    value = False
    node = property_graph.add_or_get_node(concept_prop_str, value)

    # Check property node is in graph
    assert node.key in property_graph.graph.nodes

    # Check properpty node is not in concept index since its not an
    # identifier
    concept_instances = property_graph.concept_index[node.concept]
    assert node.key not in concept_instances

    # Test get property node
    assert property_graph.get_node(node.key) == node

    # Add identifier node
    concept_prop_str = str(CONCEPT.PARTICIPANT.ID)
    value = 'P1'
    node = property_graph.add_or_get_node(concept_prop_str, value)

    # Check node is in concept index since it is an identifier
    assert node.key in concept_instances
    assert isinstance(concept_instances.get(node.key), ConceptPropertyNode)

    # Test that you cannot add a node if it already exists
    result = property_graph.add_or_get_node(concept_prop_str, value,
                                            row=5, col=5)
    assert result.row != 5
    assert result.col != 5


def test_add_edge_and_exists(property_graph):
    """
    Test add un/directed edges on ConceptPropertyGraph
    """
    # Test directed edge
    node1 = ConceptPropertyNode(str(CONCEPT.BIOSPECIMEN.TISSUE_TYPE), 'normal')
    node2 = ConceptPropertyNode(str(CONCEPT.BIOSPECIMEN.ID), 'B1')
    property_graph._add_edge(node2, node1)

    # Check nodes exist
    assert node1 == property_graph.get_node(node1.key)
    assert node2 == property_graph.get_node(node2.key)

    # Manually check networkx, does edge exist and is it only one way
    assert property_graph.graph.has_edge(node2.key, node1.key)
    assert not property_graph.graph.has_edge(node1.key, node2.key)

    # Test _edge_exists method
    assert property_graph._edge_exists(node2, node1, bidirectional=False)
    assert not property_graph._edge_exists(node2, node1, bidirectional=True)

    # Test undirected edge
    property_graph._add_edge(node1, node2)
    assert property_graph.graph.has_edge(node1.key, node2.key)

    # Test _edge_exists method
    assert property_graph._edge_exists(node1, node2, bidirectional=True)


def test_connect_id_nodes(property_graph):
    """
    Test connect_id_nodes and connect_property_node
    """

    # Create id nodes
    p1 = property_graph.add_or_get_node(str(CONCEPT.PARTICIPANT.ID), 'P1')
    b1 = property_graph.add_or_get_node(str(CONCEPT.BIOSPECIMEN.ID), 'B1')
    g1 = property_graph.add_or_get_node(str(CONCEPT.GENOMIC_FILE.ID), 'GF1')
    id_nodes = [p1, b1, g1]

    # Connect id nodes
    property_graph.connect_id_nodes(id_nodes)

    # Check if edges exist
    assert property_graph.graph.size() == 4
    assert property_graph._edge_exists(p1, b1, bidirectional=True)
    assert property_graph._edge_exists(b1, g1, bidirectional=True)

    # Test that you cannot connect two id nodes if a path already exists
    property_graph.connect_id_nodes([p1, g1])
    assert property_graph.graph.size() == 4
    assert not property_graph._edge_exists(p1, g1, bidirectional=True)


def test_connect_property_nodes(property_graph):
    """
    Test connect_id_nodes and connect_property_node
    """
    # Create property node
    tissue_type = property_graph.add_or_get_node(
        str(CONCEPT.BIOSPECIMEN.TISSUE_TYPE),
        'Normal')

    # Create id nodes
    p1 = property_graph.add_or_get_node(str(CONCEPT.PARTICIPANT.ID), 'P1')
    b1 = property_graph.add_or_get_node(str(CONCEPT.BIOSPECIMEN.ID), 'B1')
    id_nodes = [p1, b1]

    # Connect property nodes
    property_graph.connect_property_node(tissue_type, id_nodes)

    # Test that tissue type has no outgoing edges and 2 incoming edges
    assert property_graph.graph.out_degree(tissue_type.key) == 0
    assert property_graph.graph.in_degree(tissue_type.key) == 2
    for node in id_nodes:
        assert property_graph._edge_exists(node, tissue_type)
