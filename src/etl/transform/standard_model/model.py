from copy import deepcopy

import pandas as pd

from etl.transform.standard_model.graph import ConceptGraph
from etl.transform.standard_model.concept_schema import (
    concept_from,
    concept_attr_from,
    unique_key_composition as DEFAULT_KEY_COMP,
    UNIQUE_ID_ATTR,
    DELIMITER
)
from etl.configuration.log import create_default_logger


class StandardModel(object):

    def __init__(self, logger=None):
        # If we're in stand alone mode (testing)
        if not logger:
            self.logger = create_default_logger(__name__)
        # If this is called from transform stage
        else:
            self.logger = logger

        self.concept_graph = None

    def transform(self, target_api_config, target_concepts_to_transform=None):
        """
        Transform the data in the standard model into a form defined by the
        target_api_config.

        Output the transformed data in a dict where a key is the target
        concept name and the corresponding value is a list of dicts
        each of which represent a target concept instance.

        :param target_api_config: A TargetAPIConfig object containing all
        necessary configuration (target concept schemas, concept relations,
        etc.) needed to perform the transformation.
        :param target_concepts_to_transform: List of strings containing
        target concept names. Transformation will only occur for thse target
        concepts.
        """
        transformed_data = {}

        # Use whole concept set if target concepts are not supplied
        if not target_concepts_to_transform:
            target_concepts_to_transform = (target_api_config
                                            .concept_schemas.keys())

        # The schemas of the target concepts
        schemas = target_api_config.concept_schemas
        # The networkx graph containing how target concepts are related
        relation_graph = target_api_config.relationship_graph

        # For each supplied target concept
        for target_concept in target_concepts_to_transform:
            # Not found in target api config
            if target_concept not in schemas:
                fp = target_api_config.config_filepath
                self.logger.warning(
                    f'Supplied entity "{target_concept}" does not '
                    'exist in target concept schemas. Check defined concepts '
                    f'in {fp}')
                continue

            # Build target concept instances
            schema = schemas[target_concept]
            concept_instances = self._build_concept_instances(target_concept,
                                                              schema,
                                                              relation_graph)
            transformed_data.setdefault(target_concept,
                                        []).extend(concept_instances)

        return transformed_data

    def _build_concept_instances(self, target_concept, schema, relation_graph):
        """
        Build target concept instances for a particular target concept

        :param target_concept: a string denoting the name of the target concept
        :param schema: a dict containing the properties of the target concept
        instance, whose values will be extracted from the standard model graph
        :param relation_graph: a networkx.DiGraph representing relationships
        among target concepts in the target model.
        """
        # Initialize list of output payloads for the target concept
        concept_instances = []

        # Get the standard concept mapped to this target concept
        standard_concept_str = schema['standard_concept']._CONCEPT_NAME

        # Get the ID nodes for this standard concept from the concept graph
        id_node_keys = self.concept_graph.id_index.get(standard_concept_str)
        if not id_node_keys:
            self.logger.warning(
                f'The concept graph does not contain any '
                f'"{standard_concept_str}" ID nodes in the graph! '
                f'Nothing to transform for "{standard_concept_str}"')
            return concept_instances

        # Build a target concept instance for each id
        for id_node_key in id_node_keys:
            output = self._build_concept_instance(id_node_key, schema,
                                                  relation_graph)
            concept_instances.append(output)

        return concept_instances

    def _build_concept_instance(self, id_node_key, schema, relation_graph):
        """
        Build an instance of the target model concept defined by schema

        To do this, make a copy of the target concept schema. Then for each
        property in the target concept schema, try to find the value for that
        property in the concept graph.

        :param id_node_key: a ConceptNode.key string containing the ID of
        the target concept instance in the concept graph
        :param schema: a dict containing the property schema for the
        target concept
        :param relation_graph: a networkx.DiGraph containing target concept
        hierarchical relations
        """
        # Make copy of schema
        output = deepcopy(schema)

        # Get ConceptNode from concept graph given the node's ID
        id_node = self.concept_graph.get_node(id_node_key)

        # Fill in the mapped id value
        output['id'] = id_node.value

        # Find values for properties
        for key, concept_attr in output.get('properties').items():
            value = self.concept_graph.find_attribute_value(id_node,
                                                            concept_attr,
                                                            relation_graph)
            output['properties'][key] = value

        # Find values for links
        links = output.get('links', {})
        for key, concept_attr in links.items():
            value = self.concept_graph.find_attribute_value(id_node,
                                                            concept_attr,
                                                            relation_graph)
            output['links'][key] = value

        return output

    def populate(self, df_dict, logger=None):
        """
        Iterate over DataFrames and insert each into the ConceptGraph

        :param df_dict: a dict of Pandas DataFrames. A key is an extract config
        URL and a value is a tuple of (source file url, DataFrame).
        """
        # Create an empty concept graph
        self.concept_graph = ConceptGraph(logger=logger)

        # Insert nodes into concept attribute graph
        # For each DataFrame
        for extract_config_url, (source_file_url, df) in df_dict.items():
            # Insert unique key columns
            df = self._add_unique_key_cols(df)
            # If no unique key columns are present raise an error.
            # This means that the dataframe does not have anything to uniquely
            # identify concepts in the data and therefore no ConceptNodes
            # can be created and inserted into the ConceptGraph.
            is_any_unique_keys = any([concept_attr_from(col) == UNIQUE_ID_ATTR
                                      for col in df.columns])
            if not is_any_unique_keys:
                raise ValueError(
                    'Error inserting dataframe into ConceptGraph! There must '
                    'be at least 1 unique key column in the DataFrame. Source '
                    'of error is {extract_config_url} : {source_file_url}'
                )

            # Insert df into graph
            self.insert_df(extract_config_url, source_file_url, df)

    def insert_df(self, extract_config_url, source_file_url, df):
        """
        Iterate over a DataFrame's cells and insert each cell as a ConceptNode
        in the ConceptGraph. Then create edges between nodes.

        :param extract_config_url: the URL of the extract config file used to
        create the df
        :param source_file_url: the URL of the source data file
        :param df: the Pandas DataFrame containing mapped data
        """

        for r, row in df.iterrows():
            id_nodes = []
            attribute_nodes = []
            for c, col in enumerate(df.columns):
                # ConceptNode's kwargs
                props = {
                    'extract_config_url': extract_config_url,
                    'source_file_url': source_file_url,
                    'row': r,
                    'col': c
                }
                # Do not add null nodes to the graph
                if pd.isnull(row[col]):
                    continue
                # Add node to graph
                node = self.concept_graph.add_or_get_node(col, row[col],
                                                          **props)
                # Sort nodes into ID nodes and attribute nodes
                if node.is_identifier:
                    id_nodes.append(node)
                else:
                    attribute_nodes.append(node)

            # Connect id nodes
            self.concept_graph.connect_id_nodes(id_nodes)

            # Connect attribute nodes
            for prop_node in attribute_nodes:
                self.concept_graph.connect_attribute_node(
                    prop_node, id_nodes
                )

    def _add_unique_key_cols(self, df,
                             unique_key_composition=DEFAULT_KEY_COMP):
        """
        Construct and insert unique key columns for each concept present in the
        mapped df. Only do this if there isn't already an existing unique key
        column for a particular concept.

        The unique key is a special standard concept attribute which is
        reserved to uniquely identify concept instances of the same type.

        If the unique key column hasn't been explicitly provided in the mapped
        data, then this method will insert a unique key column using the values
        from other columns in the data. The columns it uses are defined in
        etl.transform.standard_model.concept_schema.unique_key_compositionself.

        A unique key column for a concept will only be added if all of the
        columns required to compose the unique key for that concept exist in
        the DataFrame.

        The value of a unique key will be a delimited string containing the
        values from required unique key columns.

        For example, given the mapped dataframe:

            CONCEPT.OUTCOME.ID | CONCEPT.OUTCOME.VITAL_STATUS
            -------------------------------------------------
                OT1                 Deceased

        the unique_key_composition:
                {
                    CONCEPT.OUTCOME: {CONCEPT.OUTCOME.ID,
                                      CONCEPT.OUTCOME.VITAL_STATUS}
                }

        and the unique key delimiter: -

        the output dataframe would be:

            CONCEPT.OUTCOME.ID | CONCEPT.OUTCOME.VITAL_STATUS | CONCEPT.OUTCOME.UNIQUE_KEY # noqa E501
            ------------------------------------------------------------------------------ # noqa E501
                OT1                 Deceased                        OT1-Deceased # noqa E501

        :param df: the Pandas DataFrame that will be modified
        :param unique_key_composition: a dict where a key is a standard concept
        string and a value is a list of required columns needed to compose
        a unique key for the concept.
        """
        # Iterate over all concepts and try to insert a unique key column
        # for each concept
        for concept_name in unique_key_composition.keys():
            # Determine the cols needed to compose a unique key for the concept
            output_key_cols = []
            try:
                self._unique_key_cols(concept_name,
                                      df, unique_key_composition,
                                      output_key_cols)
            except AssertionError as e:
                # Unique key composition not defined for a concept, dev error
                if 'key composition not defined' in str(e):
                    raise e
                # One of the required cols to make the unique key did not exist
                # Cannot add a unique key col for this concept, move on
                self.logger.debug(str(e))
            else:
                # Insert unique key column for the concept
                unique_key_col = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
                df[unique_key_col] = df.apply(
                    lambda row: DELIMITER.join([str(row[c])
                                                for c in output_key_cols]),
                    axis=1)

        return df

    def _unique_key_cols(self, concept_name, df, unique_key_composition,
                         output_key_cols):
        """
        Compose the list of column names that are needed to build a unique key
        for a particular concept.

        The required columns for a concept's unique key are defined in
        etl.transform.standard_model.concept_schema.unique_key_composition.

        A concept's unique key can be composed of other concept's unique keys.
        This is a recursive method that collects the required columns needed to
        build a unique key column for a concept. If one of the required columns
        is a unique key itself, then the method will recurse in order to get
        the columns that make up that unique key.

        :param concept_name: a string and the name of the concept for which a
        unique key will be made
        :param df: a Pandas DataFrame
        :param output_key_cols: the output list of columns needed to build the
        unique key column for a concept.
        """
        # If unique key col for this concept already exists return that
        output_key_col_name = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
        if output_key_col_name in df.columns:
            output_key_cols.append(output_key_col_name)
            return

        # Get the required cols needed to make a unique key for this concept
        required_cols = unique_key_composition.get(concept_name)
        # If required cols don't exist for a concept, then we have made a dev
        # error in concept_schema.py
        assert required_cols, ('Unique key composition not defined in concept '
                               f'schema for concept {concept_name}!')

        # Add required cols to cols needed for the unique key
        cols = set(df.columns)
        for req_col in required_cols:
            if concept_attr_from(req_col) == UNIQUE_ID_ATTR:
                # The required col is a unique key itself, so recurse
                self._unique_key_cols(concept_from(req_col),
                                      df, unique_key_composition,
                                      output_key_cols)
            else:
                # If all of the required cols are not present then we cannot
                # make the unique key
                assert req_col in cols, ('Did not create unique key for '
                                         f'{concept_name}. Missing 1 or more '
                                         'required columns')
                output_key_cols.append(req_col)
