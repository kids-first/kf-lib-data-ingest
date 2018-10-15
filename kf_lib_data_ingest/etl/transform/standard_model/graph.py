import networkx as nx

from common.misc import write_json
from etl.transform.standard_model.concept_schema import (
    DELIMITER,
    is_identifier,
    concept_from,
    concept_attr_from
)
from kf_lib_data_ingest.common.misc import iterate_pairwise
from common.misc import iterate_pairwise
from etl.configuration.log import create_default_logger


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

    def __init__(self, logger=None):
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

        # Standalone mode
        if not logger:
            self.logger = create_default_logger(__name__)
        # Called from standard_model.populate
        else:
            self.logger = logger

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

    def find_attribute_value(self, start_node, concept_attribute_str,
                             relation_graph):
        """
        Search the concept graph for the value of the concept attribute,
        starting at a particular node.

        Use breadth first search to traverse the graph. Do not pass thru
        certain nodes that are "invalid", as governed by the relation_graph.
        See _is_neighbor_valid for more details on what constitutes validity.

        :param start_node: A ConceptNode and the starting point for search
        :param concept_attribute_str: A string containing the concept attribute
         (i.e. CONCEPT|PARTICIPANT|RACE) for which we must find the value.
        :param relation_graph: a networkx.DiGraph containing the rules for
        traversing the graph when searching for the concept attribute value.
        """
        # The start node does not exist in the graph
        if not self.graph.has_node(start_node.key):
            raise ValueError(f'ConceptNode {start_node.key} not found! '
                             'Cannot continue find operation.')

        # Does graph contain any nodes with concept attribute concept_attr_str?
        if is_identifier(concept_attribute_str):
            exists = (self.id_index.get(concept_from(concept_attribute_str))
                      is not None)
        else:
            exists = concept_attribute_str in self.attribute_index

        if not exists:
            self.logger.info(
                f'Could not find a value for {concept_attribute_str} since '
                f'there are 0 {concept_attribute_str} nodes in the concept '
                'graph!')

            return None

        # -- Search the graph for the value of concept attribute --
        # Init data structures
        # Keep track of nodes visited
        visited = set([start_node.key])
        # Keep track of nodes to process
        queue = [start_node]

        # Check directly connected concept ID nodes before searching graph
        # Value for a given concept attribute is likely to be directly
        # connected to its concept ID node since attributes for a particular
        # concept are typically in the same table as the IDs for that concept
        for node_key in nx.neighbors(self.graph, start_node.key):
            neighbor = self.get_node(node_key)
            if neighbor.concept_attribute_pair == concept_attribute_str:
                return neighbor.value

        # Start breadth first search
        while queue:
            current = queue.pop(0)

            # Found the node with the value for this concept attr
            if current.concept_attribute_pair == concept_attribute_str:
                return current.value

            # Not found - keep searching neighbors
            for node_key in nx.neighbors(self.graph, current.key):
                neighbor = self.get_node(node_key)
                # Add neighbor to list if it has not been searched yet and
                # the neighbor is valid for traversal
                concept_str = concept_from(concept_attribute_str)
                neighbor_valid = self._is_neighbor_valid(concept_str,
                                                         neighbor,
                                                         relation_graph)
                if (neighbor.key not in visited) and neighbor_valid:
                    queue.append(neighbor)
                    visited.add(neighbor.key)

        # Searched the entire graph and we did not find the value for
        # concept_attr_str
        self.logger.info(f'Could not find a value for {concept_attribute_str}'
                         ' in the concept graph.')
        return None

    def _is_neighbor_valid(self, node_concept, neighbor, relation_graph):
        """
        Helper method for find_attribute_value. Determines whether or not a
        neighbor node is valid for traversal during a search.

        Given the standard concept of a node N in the concept graph, and a
        neighbor of N, determine whether the neighbor node is allowed to
        be added to the list of nodes to search.

        Validity
        *********
        A neighbor is valid if it's concept is not an ancestor concept of
        node_concept. Ancestory is defined in relation_graph, which stores
        the hierarchical relationships between concepts.

            Example 1 - Is neighbor valid, given:

                - node_concept = participant
                - neighbor node = biospecimen B1
                - relation_graph: {
                    family: {participant},
                    participant: {biospecimen},
                }

                This is asking - is B1 valid for traversal while searching for
                the value of a participant concept attribute?

                Yes, since B1's concept, biospecimen, is a not an ancestor of
                concept participant

        If a neighbor's concept IS an ancestor concept of node_concept, then
        a second check is needed to determine if the neighbor is valid.
        A neighbor with an ancestor concept, wrt node_concept, is valid if
        it is not connected to any nodes whose concept is equal to node_concept
        or whose concept is any of node_concept's descendant concepts.

            Example 2 - Is neighbor valid, given:

                - node_concept = participant
                - neighbor node = family F1
                - relation_graph: {
                    family: {participant},
                    participant: {biospecimen},
                }

            This is asking is F1 valid for traversal while searching for the
            value of a participant concept attribute?

            Maybe - F1's concept, family, is an ancestor of concept participant
            For F1 to be valid, it cannot be connected to any other participant
            concepts or descendant concepts, such as biospecimen. If F1 is
            connected to more than one participant, and we allow traversal
            through F1 when searching for a particular participant's attribute
            value, then we may find the value but for the wrong participant.

        :param node_concept: A string that is the standard concept of a node N
        :param neighbor: A ConceptNode that is one of N's neighbors
        :param relation_graph: A networkx.DiGraph governing how the graph can
        be traversed when searching for the value of a concept attribute.
        """
        # Ancestor concepts of node_concept
        ancestors = [ancestor for ancestor
                     in nx.ancestors(relation_graph, node_concept)]

        # Valid - the neighbor is not in the set of ancestors
        if neighbor.concept not in set(ancestors):
            return True

        # Check if the neighbor is connected to other nodes with the same
        # concept as node_concept or if it is connected to nodes
        # with a concept that is one of the descendant concepts of node_concept
        descendants = [descendant for descendant
                       in nx.descendants(relation_graph, node_concept)]

        restrictions = set(descendants + [node_concept])

        # Start the breadth first search
        visited = set([neighbor.key])
        queue = [neighbor]

        while queue:
            # Is this a restricted concept?
            current = queue.pop(0)
            if current.concept in restrictions:
                return False

            # Add neighbor nodes if they haven't been visited and
            # they are identifier nodes. No need to look at non-identifier
            # nodes since they are dead ends (have no outgoing edges)
            for node_key in nx.neighbors(self.graph, current.key):
                node = self.get_node(node_key)
                if (node.key not in visited) and node.is_identifier:
                    queue.append(node)
                    visited.add(node.key)
        return True

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

    def export_to_file(self, filepath='./concept_graph.json'):
        """
        Serialize the concept graph and write to a JSON file so that it can
        later be loaded into a neo4j graph database for better
        visualization/debugging.

        Loading of the graph is done by utilities.neo4j_util.Neo4jGraph.

        Content follows this format:
        {
            'nodes': {
                'node_id_here': {
                    'label': 'standard concept here',
                    'attributes': {
                        'attribute name': 'value of concept attribute',
                        'is_identifier': True/False
                        ...
                    }
                }
            },
            'edges': [
                {
                    'label': 'has_property' or 'connected_to',
                    'attributes': {

                    },
                    'source': 'node1 id here',
                    'target': 'node1 id here'
                }
            ]
        }

        :param graph: a networkx.DiGraph containing the standard concept graph
        :param filepath: the path to the file where the output will be written
        """
        output = {'nodes': {}, 'edges': []}
        # Nodes
        for key, node_attrs in self.graph.nodes.items():
            concept_node = node_attrs['object']
            output['nodes'][key] = {
                'attributes': {
                    'name': key,
                    'concept': concept_node.concept,
                    'attribute': concept_attr_from(
                        concept_node.concept_attribute_pair),
                    'value': concept_node.value,
                    'is_identifier': concept_node.is_identifier
                }
            }

        # Edges
        for edge in self.graph.edges:
            source = edge[0]
            target = edge[1]
            concept_node = self.graph.nodes[target]['object']
            if concept_node.is_identifier:
                label = 'connected_to'
            else:
                label = 'has_property'
            output['edges'].append(
                {
                    'label': label,
                    'source': source,
                    'target': target
                }
            )

        # Write output to file
        write_json(output, filepath)


class ConceptNode(object):
    """
    A ConceptNode represents a mapped cell (mapped column name and cell
    value) from a source data table.
    """

    def __init__(self, concept_attribute, value, extract_config_url='',
                 source_file_url='', row='', col=''):
        """
        A ConceptNode represents a mapped cell from a source data
        table. Construction requires the standard concept and attribute
        that the cell's column name was mapped to, and the value of the cell.

        :param concept_attribute: a serialized concept attribute from
        standard_model.concept_schema. This will be a string.
        :param value: the value of the concept attribute
        :param source_file_url: an optional parameter. This is the location of
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
        self.concept = concept_from(concept_attribute)
        self.attribute = concept_attr_from(concept_attribute)
        self.concept_attribute_pair = concept_attribute
        self.value = str(value)
        self.key = self._create_key()

        # Is node an identifier
        self.is_identifier = self._set_is_identifier()

        # Uid parameters
        self.extract_config_url = extract_config_url
        self.source_file_url = source_file_url
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
                self.source_file_url and self.row and self.col):
            return DELIMITER.join([self.extract_config_url,
                                   self.source_file_url,
                                   self.row,
                                   self.col])
        else:
            return None
