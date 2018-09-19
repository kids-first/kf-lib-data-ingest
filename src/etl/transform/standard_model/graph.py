import networkx as nx

from etl.transform.standard_model.concept_schema import (
    DELIMITER,
    is_identifier
)
from common.misc import iterate_pairwise


class ConceptGraph(object):
    """
    A directed graph representing the source data in a standard form.

    The ConceptGraph is a wrapper around networkx.DiGraph.

    Nodes
    *************
    ConceptGraph contains ConceptNodes, each of which represent
    a single mapped cell from a source data table.

    At a minimum, the ConceptNode contains the standard concept and
    concept attribute that the cell's column was mapped to and the value of the
    cell.

    These 3 things create the ConceptNode's key which is a
    delimited string that uniquely identifies the node in the graph.

        Example key: PARTICIPANT|RACE|White

    There are 2 types of ConceptNodes:
        - identifier nodes
        - non-identifier nodes or attribute nodes

    An identifier node is guaranteed to uniquely identify a single concept
    instance. Non-identifier or attribute nodes are simply nodes which are not
    guaranteed to uniquely identify any particular concept instance.

    Edges
    *************
    All nodes which came from cells in the same row of a source data table
    are considered to be directly related and will have their relationships
    represented in the graph by edges.

        - Each identifier node in a row will be connected to every
         non-identifier node in the row via 1 way edges starting from itself
         and ending at the non-identifier nodes.

         - For every pair of identifier nodes within a row, a bi-directional
          edge is created between the nodes in the pair if and only if
          a path in the graph does not already exist between the two nodes.

    Example
    *************
    Here is how an example source data table would be represented in the
    ConceptGraph.

    TABLE:
    PARTICIPANT|ID | BIOSPECIMEN|ID | PARTICIPANT|RACE
    ----------------------------------------------------------------------
            P1           B1                 White
            P2           B2                 White

    GRAPH (given a node key delimiter equal to '|'):
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
        Initialize a networkx directed graph and the indices

        The indentifier index stores a dict of dicts of identifier
        ConceptNodes, first keyed by concept and then keyed by
        ConceptNode.key

        Example:
            self.id_index = {
                CONCEPT.PARTICIPANT: {
                    PARTICIPANT|ID|P1: <ConceptNode instance>,
                    PARTICIPANT|ID|P2: <ConceptNode instance>,
                    ...
                },
                ...
            }

        The attribute index stores a dict of sets of attribute
        ConceptNodes, keyed by concept attribute. This will be used by
        standard model during transformation when searching the graph for
        attributes.

        Example:
            self. attribute_index = {
                CONCEPT.PARTICIPANT.IS_PROBAND: {
                    CONCEPT|PARTICIPANT|IS_PROBAND|True,
                    CONCEPT|PARTICIPANT|IS_PROBAND|False,
                },
                CONCEPT.DIAGNOSIS.MONDO_ID: {
                    CONCEPT|DIAGNOSIS|MONDO_ID|MONDO_5
                },
                ...
            }

        """
        self.graph = nx.DiGraph()
        self.id_index = {}
        self. attribute_index = {}

    def find_attribute_node(self, start_key, goal_key, restrictions):
        # TODO
        pass

    def add_or_get_node(self, concept_attribute, value, **kwargs):
        """
        Construct, add a ConceptNode to the graph or get existing node.

        Add the node to the id_index if it is an identifier node

        :param concept_attribute: a string referring to a standard concept
        in defined in standard_model.concept_schema.CONCEPT
        :param value: a string containing the value of the concept attribute
        :param kwargs: see ConceptNode arguments
        :returns node: the constructed ConceptNode
        """
        # Create node
        node = ConceptNode(concept_attribute, value, **kwargs)
        # Add node to internal networkx graph
        node = self._add_or_get_node(node)

        # Add node to indentifier index, if identifier. Initialize if empty.
        if node.is_identifier:
            self.id_index.setdefault(node.concept, {})[node.key] = node
        # Add node to attributes index, if not identifier. Initialize if empty.
        else:
            self.attribute_index.setdefault(node.concept_attribute_pair,
                                            set()).add(node)

        return node

    def get_node(self, node_key):
        """
        Get the ConceptNode from the graph

        :param node_key: The key of a ConceptNode
        """
        # Get node from the internal networkx graph
        nx_node = self.graph.node.get(node_key)

        # Get ConceptNode from networkx node's attribute dict
        if nx_node:
            return nx_node.get('object')
        else:
            return None

    def connect_id_nodes(self, nodes):
        """
        Connect identifier nodes via edges

        For all node pairs, create a bidirectional edge between nodes in a
        pair, if and only if there does not already exist a path between the
        nodes.

        :param nodes: a list of identifier ConceptNodes
        """
        for node1, node2 in iterate_pairwise(nodes):
            if not nx.has_path(self.graph, node1.key, node2.key):
                self._add_edge(node1, node2, bidirectional=True)

    def connect_attribute_node(self, attribute_node, id_nodes):
        """
        Create directed edges from each id node to the attribute node.

        For each edge, create the edge starting from the identifier node
        and ending at the attribute node.

        :param attribute_node: a non-identifier ConceptNode
        :param id_nodes: a list of identifier ConceptNodes
        """
        for node in id_nodes:
            self._add_edge(node, attribute_node)

    def _add_edge(self, node1, node2, bidirectional=False):
        """
        Add an un/directed edge to the graph.

        If bidirectional=False, add edge from node1 to node2
        If bidirectional=True, add edge from node1 to node2 and add edge from
        node2 to node1.

        :param node1: A ConceptNode
        :param node2: A ConceptNode
        :param bidirectional: A boolean specifying whether to add edge in both
        directions.
        """
        # Add nodes if they do not exist or get existing nodes
        self._add_or_get_node(node1)
        self._add_or_get_node(node2)

        # Add edge from node1 to node2
        self.graph.add_edge(node1.key, node2.key)
        # Add edge from node2 to node1
        if bidirectional:
            self.graph.add_edge(node2.key, node1.key)

    def _edge_exists(self, node1, node2, bidirectional=False):
        """
        Check whether an un/directed edge exists in the graph.

        If bidirectional=False, check if edge from node1 to node2 exists
        If bidirectional=True, check if edge from node1 to node2 exists and
        check if edge from node2 to node1 exists

        :param node1: a ConceptNode
        :param node2: a ConceptNode
        :param directed: A boolean specifying to check for edges in both
        directions.
        """
        edge1_exists = self.graph.has_edge(node1.key, node2.key)
        if not bidirectional:
            return edge1_exists
        else:
            edge2_exists = self.graph.has_edge(node2.key, node1.key)
            return (edge1_exists and edge2_exists)

    def _add_or_get_node(self, node):
        """
        Add node to internal networkx graph or get node if it exists.

        Nodes in a networkx graph may be strings, ints, or hashable objects
        A networkx node may also have a dict of key, value pairs associated
        with it.

        Here we use the ConceptNode.key as the node in the
        networkx graph. The key also identifies the node in the graph.
        We also store the ConceptNode instance in the node's attributes
        dict:

            {'object': ConceptNode instance}

        This is useful so that we can enforce an interface on the
        nodes in the networkx graph, and calling code may get attributes of the
        node by simply referencing the ConceptNode attributes.

        Essentially, we want this:
            node = self.graph[PARTICIPANT|ID|P1].get('object')
            print(node.key)
            print(node.row)

        instead of this:
            node = self.graph[PARTICIPANT|ID|P1]
            print(node.get('key'))
            print(node.get('row'))

        :param node: a ConceptNode
        """
        # Add node if it does not exist
        if not self.graph.has_node(node.key):
            self.graph.add_node(node.key, object=node)
            ret = node
        # Return existing node if it exists
        else:
            ret = self.get_node(node.key)

        return ret


class ConceptNode(object):
    """
    A ConceptNode represents a mapped cell (mapped column name and cell
    value) from a source data table.
    """

    def __init__(self, concept_attribute, value, extract_config_url='',
                 source_url='', row='', col=''):
        """
        A ConceptNode represents a mapped cell from a source data
        table. Construction requires the standard concept and attribute
        that the cell's column name was mapped to, and the value of the cell.

        :param concept_attribute: a serialized concept attribute from
        standard_model.concept_schema. This will be a string.
        :param value: the value of the concept attribute
        :param source_url: an optional parameter. This is the location of the
        the source data file from which the node originated.
        :param extract_config_url: an optional parameter. This is the location
        of the extract config file that references the source data file, from
        which the node originated.
        :param row: an optional parameter. This is the numeric row index in the
        source data file from which the node originated.
        :param col: an optional parameter. This is the numeric column index in
        the source data file from which the node originated.

        Derived Attributes
        *************************
        key - A ConceptNode is uniquely identified in the graph using
        its node key, which is constructed from the concept_attribute and value

            Keys look like this: BIOSPECIMEN|TISSUE_TYPE|Normal.
            The key delimiter is defined in the concept schema.

        is_identifier - A ConceptNode is an identifier node if
        its attribute is in the set of identifiers defined in concept schema.
        An identifier node is able to uniquely identify concept instances that
        share the same concept. For example the ConceptNode with
        key: PARTICIPANT|ID|P1, identifies a PARTICIPANT concept instance
        with the ID, P1.

        uid - Another unique identifier of the node. The uid is constructed
        using the url of the extract config file, the url of the source data
        file, numeric row index, and numeric column index of the node's
        source data table.

        A uid looks like this:

                /path/extract_config.py|/path/source_data_file.py|0|5

        The uid string delimiter is defined in the concept schema.

        """
        # Key parameters
        self.concept = DELIMITER.join(concept_attribute.split(DELIMITER)[0:-1])
        self.attribute = concept_attribute.split(DELIMITER)[-1]
        self.concept_attribute_pair = concept_attribute
        self.value = str(value)
        self.key = self._create_key()

        # Is node an identifier
        self.is_identifier = self._set_is_identifier()

        # Uid parameters
        self.extract_config_url = extract_config_url
        self.source_url = source_url
        self.row = str(row)
        self.col = str(col)
        self.uid = self._create_uid()

    def _set_is_identifier(self):
        """
        Set is_identifier if the ConceptNode's attribute is in the set
        of defined concept identifiers.
        """
        return is_identifier(self.concept_attribute_pair)

    def _create_key(self):
        """
        Create the key used to identify the node by concept,
        attribute and value.
        """

        return DELIMITER.join([self.concept_attribute_pair, self.value])

    def _create_uid(self):
        """
        Create a unique identifier for the node using location attributes.

        Use the extract config file URL, source data file URL,
        source data table row index, and the source data table column index.
        """
        if (self.extract_config_url and
                self.source_url and self.row and self.col):
            return DELIMITER.join([self.extract_config_url,
                                   self.source_url,
                                   self.row,
                                   self.col])
        else:
            return None
