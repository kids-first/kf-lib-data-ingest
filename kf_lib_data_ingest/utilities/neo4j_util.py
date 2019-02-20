#!/usr/bin/env python


"""
A standalone CLI utility to load a serialized ConceptGraph into a
Neo4j graph database for the purpose of deeper debugging and visualization of
the standard concept graph.

Loader expects a GML file that was written by
etl.transform.standard_model.graph.export_to_gml.

* Requires a Neo4j server to be running.

After running this utility, the concept graph can be viewed and queried in
the browser at <host>:<port>/browser, where host and port are the IP and port
number of the running Neo4j server.

See https://neo4j.com/docs/developer-manual for details on how to use the neo4j
browser to query the database using Cypher query language and visualize the
graph.
"""

import argparse

from py2neo import Graph
from py2neo.data import (
    Node,
    Relationship,
    Subgraph
)

from kf_lib_data_ingest.etl.transform.standard_model.graph import (
    ConceptNode,
    import_from_gml
)
from kf_lib_data_ingest.common.concept_schema import (
    DELIMITER as CONCEPT_DELIMITER,
    concept_attr_from
)

NEO4J_DELIMITER = '_'


class Neo4jConceptGraphLoader(object):
    def __init__(self, uri=None, username=None, password=None):
        """
        Create a neo4j Graph obj which manages the connection to the
        neo4j server.

        :param uri: the URI of the database. If None, Graph will default to the
        neo4j default server URI, bolt://localhost:7687
        :param username: database user name
        :param password: database password for username
        """
        self.graph = Graph(uri, auth=(username, password))

    def load_gml(self, filepath):
        """
        Create a neo4j graph given a GML file that was created by
        etl.transform.standard_model.graph.export_to_gml.

        This operation is idempotent. The existing neo4j graph will be deleted
        and a new one will be created to replace the deleted one. All nodes
        and relationships are created and then added to a neo4j Subgraph. The
        subgraph is then committed to the database in one transaction.

        :param filepath: path to location of serialized ConceptGraph JSON file
        """
        # Clear concept graph db
        self.graph.delete_all()
        assert len(self.graph.nodes) == 0

        # Read data into a networkx.DiGraph containing a ConceptGraph
        nx_concept_graph = import_from_gml(filepath)

        # Begin transaction
        tx = self.graph.begin()

        # For each edge
        self.neo4j_nodes = {}
        g = None
        for edge in nx_concept_graph.edges():
            # Create first neo4j Node if it doesn't exist
            source_concept_node = nx_concept_graph.node[edge[0]]['object']
            n1 = self._create_or_get_node(source_concept_node)

            # Create second neo4j node if it doesn't exist
            target_concept_node = nx_concept_graph.node[edge[1]]['object']
            n2 = self._create_or_get_node(target_concept_node)

            r = self._create_edge(n1, n2)

            # Create neo4j subgraph if it doesn't exist
            if not g:
                g = Subgraph([n1, n2], [r])
            # Add nodes and edge to neo4j subgraph
            else:
                g_part = Subgraph([n1, n2], [r])
                g = g | g_part

        # Create graph transaction and commit
        tx.create(g)
        tx.commit()

    def _create_edge(self, source_node, target_node):
        """
        Helper method to create a Neo4j edge from two neo4j concept nodes
        """
        if target_node.get('is_identifier'):
            label = 'connected_to'
        else:
            label = 'has_property'

        return Relationship(source_node, label, target_node)

    def _create_or_get_node(self, concept_node):
        """
        Helper method for Neo4jGraphLoader.load

        Create a neo4j Node if it doesn't exist in the data dict containing
        the data for all the nodes and edges to be inserted into the neo4j
        graph. If it exists, return a reference to the Node obj instead.

        :param concept_node: A ConceptNode
        """
        neo4j_node = self.neo4j_nodes.get(concept_node.key)

        if not neo4j_node:
            # Neo4j node attributes
            attr_dict = ConceptNode.to_dict(concept_node)
            pair = attr_dict.pop('concept_attribute_pair')
            key = attr_dict.pop('key')
            attr_dict['name'] = key
            attr_dict['attribute'] = concept_attr_from(pair)

            # Neo4j node label
            label = concept_node.concept

            # For some reason neo4j does only accepts an underscore as a
            # delimiter for node or relationship labels.
            neo4j_node = Node(label.replace(CONCEPT_DELIMITER,
                                            NEO4J_DELIMITER), **attr_dict)
            self.neo4j_nodes[concept_node.key] = neo4j_node

        return neo4j_node


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath",
                        help='Path to the GML file containing the serialized '
                        'ConceptGraph')

    parser.add_argument("--host", type=str,
                        help="IP address of the running Neo4j server. If not "
                        "specified will default to 127.0.0.1")

    parser.add_argument("--port", type=str,
                        help="Port of the running Neo4j server. If not "
                        "specified will default to Neo4j default port: 7687")

    parser.add_argument("-u", "--username", type=str,
                        help="Username to authenticate with Neo4j server")

    parser.add_argument("-p", "--password", type=str,
                        help="Password to authenticate with the Neo4j server")

    args = parser.parse_args()

    uri = None
    uname = args.username or None
    pword = args.password or None

    if args.host and args.port:
        uri = f'bolt://{args.host}:{args.port}'

    neo4j = Neo4jConceptGraphLoader(uri=uri, username=uname, password=pword)
    neo4j.load_gml(args.filepath)
