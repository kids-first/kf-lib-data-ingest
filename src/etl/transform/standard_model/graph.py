import networkx as nx

from etl.transform.standard_model import concept_schema
from common.misc import iterate_pairwise


class ConceptPropertyGraph(object):
    """
    A directed graph representing the source data in a standrd form.

    The ConceptPropertyGraph implements this graph and is
    a wrapper around networkx.DiGraph().

    Nodes
    *************
    ConceptPropertyGraph contains ConceptPropertyNodes, each of which represent
    a single cell from a source data table.

    The ConceptPropertyNode contains the standard concept that the cell's
    column was mapped to, the standard concept property that the cell's column
    was mapped to, and the value of the cell.

    These 3 things, create the ConceptPropertyNode's key which may uniquely
    identify the node in the graph. (Example key: PARTICIPANT|RACE|White)

    There are 2 types of ConceptPropertyNodes:
        - identifier nodes
        - non-identifier nodes

    An identifier node can uniquely identify a concept instance. The criteria
    to be an identifier is that the node must have a standard concept property
    which is in the set of identifier concept properties defined in the concept
    schema. Non-identifier nodes are self-explanatory.

    Edges
    *************
    All nodes which came from cells in the same row of a source data table,
    are considered to be directly related and must have their relationships
    represented in the graph by edges.

        - Each identifier node in a row is connected to every non-identifier
         node, via 1 way edges starting from itself and ending at the
         non-identifier nodes.

         - For every pair of identifier nodes within a row, a bi-directional
          edge is created, if and only if, a path in the graph does not already
          exist between the two nodes.

    Example
    *************
    Here is how an example source data table would be represented in the
    ConceptPropertyGraph.

    TABLE:
    PARTICIPANT|ID | BIOSPECIMEN|ID | PARTICIPANT|RACE
    ----------------------------------------------------------------------
            P1           B1                 White
            P2           B2                 White

    GRAPH:
    {
        PARTICIPANT|ID|P1: {BIOSPECIMEN|ID|B1,
                            PARTICIPANT|RACE|White},
        PARTICIPANT|ID|P2: {BIOSPECIMEN|ID|B2,
                            PARTICIPANT|RACE|White},
        BIOSPECIMEN|ID|B1: {PARTICIPANT|ID|P1,
                            PARTICIPANT|RACE|White},
        BIOSPECIMEN|ID|B2: {PARTICIPANT|ID|P2,
                            PARTICIPANT|RACE|White},
        PARTICIPANT|RACE|White: {}
    }

    """

    def __init__(self):
        """
        Initialize a networkx directed graph and concept index

        The concept index stores a dict of dicts of node, first keyed by
        concept and then keyed by node key

        Example:
            self.concept_index = {
                CONCEPT.PARTICIPANT: {
                    PARTICIPANT|ID|P1: ConceptPropertyNode,
                    PARTICIPANT|ID|P2: ConceptPropertyNode,
                    ...
                },
                ...
            }

        """
        self.graph = nx.DiGraph()
        self.concept_index = {}

    def find_property_node(self, start_key, goal_key, restrictions):
        pass

    def add_node(self, concept_property, value, **kwargs):
        """
        Construct a ConceptPropertyNode and add it to the graph and
        concept_index if it is an identifier node

        :param concept_property: a string referring to a standard concept
        in defined in standard_model.concept_schema.CONCEPT
        :param value: a string containing the value of the concept property
        :returns node: the constructed ConceptPropertyNode
        """
        node = ConceptPropertyNode(concept_property,
                                   value,
                                   **kwargs)
        success = self._add_node(node)
        if success:
            # Initialize concept index if empty
            self.concept_index.setdefault(node.concept, {})

            # Add node to concept index, if identifier
            if node.is_identifier:
                self.concept_index[node.concept][node.key] = node
            return node
        else:
            return None

    def get_node(self, node_key):
        """
        Get the ConceptPropertyNode from the graph

        :param node_key: The key of a ConceptPropertyNode
        """
        # Get networkx node in graph
        nx_node = self.graph.node.get(node_key)

        # Get ConceptPropertyNode from networkx node's key, value pairs
        if nx_node:
            return nx_node.get('object')
        else:
            return None

    def connect_id_nodes(self, nodes):
        """
        For all node pairs, create a bidirectional edge between the nodes,
        if and only if there does not already exist a path between the nodes.

        :param nodes: a list of identifier ConceptPropertyNodes
        """
        for node1, node2 in iterate_pairwise(nodes):
            if not nx.has_path(self.graph, node1.key, node2.key):
                self._add_edge(node1, node2, bidirectional=True)

    def connect_property_node(self, property_node, id_nodes):
        """
        Create directed edges from each id node to the property node.

        For each edge, create it starting from the identifier node
        and ending at the property node.

        :param property_node: a non-identifier ConceptPropertyNode
        :param id_nodes: a list of identifier ConceptPropertyNodes
        """
        for node in id_nodes:
            self._add_edge(node, property_node)

    def _add_edge(self, node1, node2, bidirectional=False):
        """
        Add an un/directed edge to the graph if it does not already exist.

        If bidirectional=True, add edge from node1 to node2 and add edge from
        node2 to node1.

        :param node1: A ConceptPropertyNode
        :param node2: A ConceptPropertyNode
        :param bidirectional: A boolean specifying whether to add edge in both
        directions.
        """
        # Add nodes if not exist
        self._add_node(node1)
        self._add_node(node2)

        # Check if edge exists
        if not self._edge_exists(node1, node2, bidirectional=bidirectional):
            # Add edge from node1 to node2
            self.graph.add_edge(node1.key, node2.key)

            # Add edge from node2 to node1
            if bidirectional:
                self.graph.add_edge(node2.key, node1.key)

    def _edge_exists(self, node1, node2, bidirectional=False):
        """
        Check whether an edge exists in the graph.

        :param node1: a ConceptPropertyNode
        :param node2: a ConceptPropertyNode
        :param directed: A boolean specifying to check for edges in both
        directions.
        """
        edge1_exists = self.graph.has_edge(node1.key, node2.key)
        if not bidirectional:
            return edge1_exists
        else:
            edge2_exists = self.graph.has_edge(node2.key, node1.key)
            return (edge1_exists and edge2_exists)

    def _add_node(self, node):
        """
        Add the contents of the ConceptPropertyNode to the networkx.DiGraph
        graph. Return whether operation was successful.

        Nodes in a networkx graph may be strings, ints, or any hashable object.
        A networkx node may also have a dict of key, value pairs associated
        with it.

        Here we use the ConceptPropertyNode.key to identify the node in the
        networkx graph, and we store the ConceptPropertyNode instance
        in a dict with just one key, value pair:

            {object: ConceptPropertyNode instance}

        This is useful so that we can enforce an interface on the
        nodes in the networkx graph and calling code may get attributes of the
        node by simply referenceing the ConceptPropertyNode attributes.

        Essentially, we want this:
            node = self.graph[PARTICIPANT|ID|P1].get('object')
            node.key
            node.row
        instead of this:
            node = self.graph[PARTICIPANT|ID|P1]
            node.get('key')
            node.get('row')

        :param node: a ConceptPropertyNode
        """
        success = False
        if not self.graph.has_node(node.key):
            self.graph.add_node(node.key, **{'object': node})
            success = True

        return success


class ConceptPropertyNode(object):
    """
    A ConceptPropertyNode represents a cell from a source data table.
    """

    def __init__(self, concept_property, value, **kwargs):
        """
        A ConceptPropertyNode represents a cell from a source data table.
        It must be constructed with the standard concept and property that the
        cell's column was mapped to, and the value of the cell.

        :param concept_property: a serialized concept property from
        standard_model.concept_schema. This will be a string.
        :param value: the value of the concept property
        :param source_url: an optional parameter. This is the location of the
        the source data file from which the node originated.
        :param extract_config_url: an optional parameter. This is the location
        of the extract config file that references the source data file, from
        which the node originated.
        :param row: an optional parameter. This is the numeric row index in the
        source data file from which the node originated.
        :param col: an optional parameter. This is the numeric column index in
        the source data file from which the node originated.

        Constructed Attributes
        *************************
        key - A ConceptPropertyNode is uniquely identified in the graph using
        its node key, which is constructed from the concept_property and value.
        Keys look like this: BIOSPECIMEN|TISSUE_TYPE|Normal. The key delimiter
        is defined in the concept schema.

        is_identifier - A ConceptPropertyNode node is an identifier node if
        its property is in the set of identifiers defined in concept schema.
        An identifier node is able to uniquely identify concept instances that
        share the same concept. For example the ConceptPropertyNode node with
        key: PARTICIPANT|ID|P1, identifies a Participant concept instance
        with the ID P1.

        uid - Another unique identifier of

        """
        # Key parameters
        self.concept = concept_property.split(concept_schema.DELIMITER)[0]
        self.property = concept_property.split(concept_schema.DELIMITER)[1]
        self.value = str(value)
        self.key = self._create_key()

        # Is node an identifier
        self.is_identifier = self._set_is_identifier()

        # Uid parameters
        self.source_url = kwargs.get('source_url', '')
        self.extract_config_url = kwargs.get('extract_config_url', '')
        self.row = str(kwargs.get('row', ''))
        self.col = str(kwargs.get('col', ''))
        self.uid = self._create_uid()

    def _set_is_identifier(self):
        """
        Set is_identifier if the ConceptPropertyNode's property is in the set
        of defined concept identifiers.
        """
        s = concept_schema.DELIMITER.join([self.concept, self.property])
        return concept_schema.is_identifier(s)

    def _create_key(self):
        """
        Create the key used to identify the node by concept, property and value
        """
        concept_property = concept_schema.DELIMITER.join([self.concept,
                                                          self.property])

        return concept_schema.DELIMITER.join([concept_property, self.value])

    def _create_uid(self):
        """
        Create the ID used to identify the node by its position in a table
        and the URL of the extract config which produced that table.
        """
        if self.extract_config_url and self.row and self.col:
            return concept_schema.DELIMITER.join([self.extract_config_url,
                                                  self.row,
                                                  self.col])
        else:
            return None


if __name__ == '__main__':
    G = ConceptPropertyGraph()
    f1 = G.add_node('FAMILY|ID', 'F1')
    p1 = G.add_node('PARTICIPANT|ID', 'P1')
    b1 = G.add_node('BIOSPECIMEN|ID', 'B1')
    tt_normal = G.add_node('BIOSPECIMEN|TISSUE_TYPE', 'NORMAL')

    G.connect_id_nodes([f1, b1, p1])

    print(G.graph.nodes)
    print(G.graph.edges)
