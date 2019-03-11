"""
Module for transforming source data DataFrames to the standard model.
"""
import os
from abc import abstractmethod
from copy import deepcopy
from pprint import pformat

import pandas

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import (
    DELIMITER,
    UNIQUE_ID_ATTR,
    concept_attr_from,
    concept_from
)
from kf_lib_data_ingest.common.concept_schema import \
    unique_key_composition as DEFAULT_KEY_COMP
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import (
    get_open_api_v2_schema,
    read_json,
    write_json
)
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type
)
from kf_lib_data_ingest.config import (
    DEFAULT_ID_CACHE_FILENAME,
    DEFAULT_TARGET_URL
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)

VALUE_DELIMITER = '-'


class TransformStage(IngestStage):
    def __init__(self, target_api_config_path,
                 target_api_url=DEFAULT_TARGET_URL, ingest_output_dir=None,
                 uid_cache_filepath=None):

        super().__init__(ingest_output_dir=ingest_output_dir)

        self.target_api_url = target_api_url
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.uid_cache_filepath = (uid_cache_filepath or
                                   os.path.join(os.getcwd(),
                                                DEFAULT_ID_CACHE_FILENAME))

    @abstractmethod
    def _do_transform(self, data_dict):
        """
        Transform the tabular mapped data into a unified standard form,
        then transform again from the standard form into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: the output (a dict of mapped DataFrames) from
        ExtractStage.run. See TransformStage._validate_run_parameters for
        a more detailed description.
        :type data_dict: dict
        """
        pass

    def _read_output(self):
        """
        Read previously written transform stage output

        :returns: dict (keyed by target concepts) of lists of dicts
        representing target concept instances (i.e. participant, biospecimen,
        etc)
        """
        output = {
            os.path.splitext(filename)[0]: read_json(
                os.path.join(self.stage_cache_dir, filename))
            for filename in os.listdir(self.stage_cache_dir)
            if filename.endswith('.json')
        }
        self.logger.info(f'Reading {type(self).__name__} output:\n'
                         f'{pformat(list(output.keys()))}')

        return output

    def _write_output(self, output):
        """
        Write output of transform stage to JSON file

        :param output: output created by TransformStage._run
        :type output: a dict of pandas.DataFrames
        """
        assert_safe_type(output, dict)
        assert_all_safe_type(output.values(), list)
        paths = []
        for key, data in output.items():
            fp = os.path.join(self.stage_cache_dir, key + '.json')
            paths.append(fp)
            write_json(data, fp)
        self.logger.info(f'Writing {type(self).__name__} output:\n'
                         f'{pformat(paths)}')

    def _validate_run_parameters(self, data_dict):
        """
        Validate the parameters being passed into the _run method. This
        method gets executed before the body of _run is executed.

        A key in df_dict should be a string containing the URL to the
        extract config module used to produce the Pandas DataFrame in the
        value tuple.

        A value in df_dict should be a tuple where the first member is a
        string containing the URL to the source data file, and the second
        member of the tuple is a Pandas DataFrame containing the mapped
        source data.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined above.
        """
        try:
            # Check types
            assert_safe_type(data_dict, dict)
            assert_all_safe_type(data_dict.keys(), str)

            # Check that values are tuples of (string, DataFrames)
            for extract_config_url, df in data_dict.values():
                assert_safe_type(extract_config_url, str)
                assert_safe_type(df, pandas.DataFrame)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

    def handle_nulls(self, target_instances, target_schema):
        """
        Convert null property values in `target_instances` to acceptable values
        based on the type of property as defined in target schema.

        See kf_lib_data_ingest.common.misc.get_open_api_v2_schema for expected
        format of target_schema

        `target_instances` is a dict keyed by the `target_concepts` defined in
        this `target_api_config.concept_schemas`. The values are lists of
        dicts, where a dict in the list takes on the same form as the dicts in
        `target_api_config.concept_schemas`.

        For example, `target_instances` might look like:

        {
            'participant': [
                {
                    "endpoint": "/participants",
                    "id": "1",
                    "links": {
                        "family_id": None,
                        "study_id": None
                    },
                    "properties": {
                        "affected_status": None,
                        "consent_type": "GRU",
                        "ethnicity": None,
                        "external_id": "1",
                        "gender": "Female",
                        "is_proband": None,
                        "race": None,
                        "visible": None
                    }
                },
                ...
            ]
        }

        The `output` would look like:

        {
            'participant': [
                {
                    "endpoint": "/participants",
                    "id": "1",
                    "links": {
                        "family_id": None,
                        "study_id": None
                    },
                    "properties": {
                        "affected_status": None,
                        "consent_type": "GRU",
                        "ethnicity": "Not Reported",
                        "external_id": "1",
                        "gender": "Female",
                        "is_proband": None,
                        "race": "Not Reported",
                        "visible": None
                    }
                },
                ...
            ]
        }

        :param target_instances: a dict of lists containing dicts representing
        target concepts
        :param target_schema: the target service concept schemas
        :returns target_instances: Updated version of the input
        """
        version = target_schema.get('version')
        schemas = target_schema.get('definitions')
        self.logger.info(f'Do null processing using target schema '
                         f'{target_schema.get("target_service")}, '
                         f'version {pformat(version)}')

        for target_concept, list_of_instances in target_instances.items():
            # Get schema for target concept
            schema = schemas.get(target_concept)
            if not schema:
                self.logger.info(
                    f'Skip handle nulls for {target_concept}. No schema '
                    'was found.')
                continue
            # Convert nulls
            for i, instance in enumerate(list_of_instances):
                for attr, value in instance['properties'].items():
                    if value is not None:
                        continue
                    property_def = schema['properties'].get(attr)
                    mapped_value = None
                    if not property_def:
                        self.logger.warning(
                            'No property definition found for '
                            f'{target_concept}.{attr} in target schema '
                            'This property may not exist '
                            'anymore in the target service.'
                        )
                    else:
                        if property_def['type'] == 'string':
                            mapped_value = constants.COMMON.NOT_REPORTED
                            # Note - the dataservice should tell us if the
                            # format of the string is `date-time`, then we
                            # could do:
                            #   property_def.get('format') == 'date-time'.
                            # Right now it doesn't do this.
                            # That's what we should use to determine what null
                            # value to use. For now, we're just using the name
                            # of the attribute
                            if 'date' in attr:
                                mapped_value = None
                        else:
                            mapped_value = None

                    self.logger.debug(
                        f'Mapping {target_concept}.{attr} value {value} to '
                        f'{mapped_value}')

                    instance['properties'][attr] = mapped_value

    def _unique_keys_to_target_ids(self, uid_cache, target_instances):
        """
        This method preps `target_instances` for the load stage by
        translating the instances' unique keys, created in
        `insert_unique_keys`, to identifiers assigned by the target service
        in a previous ingest run.

        Each time a new concept instance is created in the target service,
        the ingest pipeline saves a record of the created instance's ID from
        the target service and the instance's unique key. These records are
        stored in the UID cache.

        The translation happens by looking up the instance's unique key
        in the UID cache.

        Then later on, if the load stage sees that the target service ID for
        an instance is None, it will know to create a NEW instance in the
        target service.

        If the load stage sees that the target service ID for an instance is
        not None, it will know that this instance exists in the target
        service and it will UPDATE the instance.

        Before ID translation, `target_instances` might look like:

            { 'biospecimen': [
                {
                    'endpoint': '/biospecimens',
                    'id': 'b1',
                    'links': {
                        'participant_id': 'p1',          <- Exists in target service # noqa E501
                        'sequencing_center_id': 'Baylor' <- Exists in target service # noqa E501
                    },
                    'properties': {
                        'analyte_type': 'dna',
                        ...
                    }
                },
                ...
                ]
            }

        After ID translation, `target_instances would look like:

            { 'biospecimen': [
                {
                    'endpoint': '/biospecimens',
                    'id': {
                        'source': 'b1',
                        'target': None                  <- Not in target service yet # noqa E501
                    },
                    'links': {
                        'participant_id': {
                            'source': 'p1',
                            'target': 'PT_00001111'     <- Found in UID cache
                        },
                        'sequencing_center_id': {
                            'source': 'Baylor',
                            'target': 'SC_00001111'     <- Found in UID cache
                        }
                    },
                    'properties': {
                        'analyte_type': 'dna',
                        ...
                    }
                },
                ...
                ]
            }

        :param uid_cache: unique ID cache. See `load_uid_cache` method
        :param target_instances: dict containing target instance dicts. See
        `_run` method
        :returns target_instances: modified version of `target_instances` input
        containing lookups of target service IDs
        """
        self.logger.info(
            'Translating unique keys to target service IDs')
        for target_concept, instances in target_instances.items():
            for instance in instances:
                # id
                cache = uid_cache.get(target_concept, {})
                source_id = instance['id']
                instance['id'] = {
                    'target': cache.get(source_id),
                    'source': source_id
                }
                # links
                links = {}
                for link_name, value in instance['links'].items():
                    cache = uid_cache.get(link_name.split('_id')[0], {})
                    links.update(
                        {
                            link_name: {
                                'target': cache.get(value),
                                'source': value
                            }
                        }
                    )
                instance['links'] = links

        return target_instances

    def _run(self, data_dict):
        """
        Transform the tabular mapped data into a dict of lists.
        Keys are target entity types and values are lists of target entity
        dicts.

        :param data_dict: a dict containing the mapped source data which
        follows the format outlined in _validate_run_parameters.
        :returns target_instances: dict - keyed by target concept -
        of lists containing dicts - representing target concept instances.
        """

        # Insert unique key columns before running transformation
        self._insert_unique_keys(data_dict)

        target_instances = self._do_transform(data_dict)

        # Null processing
        target_api_schema = get_open_api_v2_schema(
            self.target_api_url,
            list(target_instances.keys()),
            logger=self.logger
        )
        if target_api_schema:
            self.handle_nulls(target_instances, target_api_schema)
        else:
            self.logger.warning('Skipping null processing because no target '
                                'schema was found')

        # Prep for load stage
        # Translate unique keys to existing target service ids
        # Tells load stage whether to create new or update existing entity
        try:
            uid_cache = read_json(self.uid_cache_filepath)
        except FileNotFoundError as e:
            self.logger.warning(
                f'UID cache file does not exist: {self.uid_cache_filepath}')
            uid_cache = {}

        self._unique_keys_to_target_ids(uid_cache,
                                        target_instances)

        return target_instances

    def _insert_unique_keys(
        self, df_dict, unique_key_composition=DEFAULT_KEY_COMP
    ):
        """
        Iterate over mapped dataframes and insert the unique key columns
        (i.e. CONCEPT.PARTICIPANT.UNIQUE_KEY)

        Definition
        ----------

        UNIQUE_KEY is a special standard concept attribute which is
        reserved to uniquely identify concept instances of the same type.

        Purpose
        -------
        Unique keys have several purposes:

        1. A unique key acts as a primary key for the concept instance in the
        source data.

        2. The ingest pipeline uses the unique key to identify duplicate
        concept instances of the same type and drop them

        3. Each time a new concept instance is created in the target service,
           the ingest pipeline saves a record of the created instance's ID from
           the target service and the instance's UNIQUE_KEY. These records are
           stored in the ID cache.

        The ID cache is used for determining whether to create or update an
        entity in the target service each time the ingest pipeline runs. When
        ingest pipeline runs again for the same dataset, it will lookup the
        concept instance's UNIQUE_KEY in the ID cache to see if there is an
        existing target service ID for the instance. If there is, an update
        will be performed. If not, a new instance will be created.

        Construction
        ------------
        In most cases, the UNIQUE_KEY simply equates to the identifier
        (i.e. CONCEPT.PARTICIPANT.ID) assigned to the concept instance in the
        source data by the data provider.

        But some concepts don't have an explicit identifier that was given by
        the data provider. These are usually event type concepts such as
        DIAGNOSIS and PHENOTYPE observation events. For example, `Participant
        P1 was diagnosed with Ewing's Sarcoma on date X.` is a DIAGNOSIS event
        concept

        For these concepts, a unique identifier must somehow be formed using
        existing columns in the data (a.k.a other concept attributes).

        Continuing with the above example, we can uniquely identify the
        DIAGNOSIS event by combining the UNIQUE_KEY of the PARTICIPANT who has
        been given the DIAGNOSIS, the name of the DIAGNOSIS, and the some sort
        of datetime of the event

        Example - DIAGNOSIS.UNIQUE_KEY
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        Composition Formula =

            PARTICIPANT.UNIQUE_KEY + DIAGNOSIS.NAME + DIAGNOSIS.EVENT_AGE_IN_DAYS     # noqa E501

        Value of DIAGNOSIS.UNIQUE_KEY =

            P1-Ewings-Sarcoma-500

        The rules for composing UNIQUE_KEYs are defined in
        `unique_key_composition` input parameter, and if not supplied, the
        default will be loaded from kf_lib_data_ingest.common.concept_schema.

        :param df_dict: a dict of Pandas DataFrames. A key is an extract config
        URL and a value is a tuple of (source file url, DataFrame).
        :param unique_key_composition: a dict where a key is a standard concept
        string and a value is a list of required columns needed to compose
        a unique key for the concept
        :returns df_dict: a modified version of the input, with UNIQUE_KEY
        columns added to each DataFrame
        """
        self.logger.info('Begin unique key creation for standard concepts ...')
        for extract_config_url, (source_file_url, df) in df_dict.items():
            # Insert unique key columns
            self.logger.info(f'Creating unique keys for {source_file_url}...')
            df = self._add_unique_key_cols(df, unique_key_composition)

            # If no unique key columns are present raise an error.
            # This means that the dataframe does not have anything to uniquely
            # identify concepts in the data. In the case of auto transform
            # this means no ConceptNodes can be created and inserted into the
            # ConceptGraph.
            is_any_unique_keys = any(
                [
                    concept_attr_from(col) == UNIQUE_ID_ATTR
                    for col in df.columns
                ]
            )
            if not is_any_unique_keys:
                raise ValueError(
                    'No unique keys were created for table! There must '
                    'be at least 1 unique key column in a table. Zero unique '
                    'keys in a table means there is no way to identify any '
                    ' concept instances. Source of error is '
                    f'{extract_config_url} : {source_file_url}'
                )

    def _add_unique_key_cols(self, df, unique_key_composition):
        """
        Construct and insert UNIQUE_KEY columns for each concept present in the
        mapped df. Only do this if there isn't already an existing UNIQUE_KEY
        column for a particular concept.

        A UNIQUE_KEY column for a concept will only be added if all of the
        columns required to compose the UNIQUE_KEY for that concept exist in
        the DataFrame.

        The rules for composition are defined in unique_key_composition. See
        kf_lib_data_ingest.common.concept_schema._create_unique_key_composition
        for details on structure and content.

        The value of a UNIQUE_KEY will be a delimited string containing the
        values from required columns needed to compose the UNIQUE_KEY.

        For example, given the mapped dataframe:

            CONCEPT.PARTICIPANT.ID | CONCEPT.OUTCOME.VITAL_STATUS
            -----------------------------------------------------
                PT1                 Deceased

        the unique_key_composition:
                {
                    CONCEPT.PARTICIPANT: {
                        'required': [
                            CONCEPT.PARTICIPANT.ID
                        ]
                    }
                    CONCEPT.OUTCOME: {
                        'required': [
                            CONCEPT.PARTICIPANT.UNIQUE_KEY,
                            CONCEPT.OUTCOME.VITAL_STATUS
                        ]
                    }
                }

        and the unique key delimiter: -

        the output dataframe would be:

            CONCEPT.PARTICIPANT.ID | CONCEPT.OUTCOME.VITAL_STATUS | CONCEPT.OUTCOME.UNIQUE_KEY # noqa E501
            ---------------------------------------------------------------------------------- # noqa E501
                PT1                 Deceased                        PT1-Deceased # noqa E501

        :param df: the Pandas DataFrame that will be modified
        :param unique_key_composition: the rules for composing a concept's
        unique key.
        :returns df: modified version of input DataFrame with new UNIQUE_KEY
        columns inserted for each concept present in the input DataFrame.
        """
        # Iterate over all concepts and try to insert a unique key column
        # for each concept
        for concept_name in unique_key_composition:
            # Determine the cols needed to compose a unique key for the concept
            unique_key_cols = []
            required = set()
            optional = set()
            self._unique_key_cols(
                concept_name,
                df, unique_key_composition,
                unique_key_cols, required, optional
            )

            # Missing required column needed to make the unique key
            missing_req_cols = [col for col in required
                                if col not in df.columns]
            if len(missing_req_cols) > 0:
                self.logger.debug(
                    f'Could not create unique key for {concept_name}. '
                    f'Missing required columns {missing_req_cols} needed to '
                    'create the key')
                continue

            # Insert unique key column for the concept
            unique_key_col = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
            df[unique_key_col] = df.apply(
                lambda row: VALUE_DELIMITER.join(
                    [
                        str(row[c])
                        for c in unique_key_cols
                        if c in df.columns
                    ]
                ),
                axis=1
            )
        unique_concepts_found = [
            col for col in df.columns
            if col.endswith(UNIQUE_ID_ATTR)
        ]
        self.logger.info(
            f'Found {len(unique_concepts_found)} standard concepts in '
            f'table:\n{pformat(unique_concepts_found)}'
        )

        return df

    def _unique_key_cols(self, concept_name, df, unique_key_composition,
                         unique_key_cols, required_cols, optional_cols):
        """
        Compose the list of column names that are needed to build a unique key
        for a particular concept.

        A concept's unique key can be composed of other concepts' unique keys.
        This is a recursive method that collects the required columns needed to
        build a unique key column for a concept. If one of the columns
        is a unique key then the method will recurse in order to get
        the columns which make up that unique key.

        For example, given the unique key composition:

            unique_key_composition = {
                'PARTICIPANT': {
                    'required' : [
                        'PARTICIPANT|ID'
                    ]
                }
                'DIAGNOSIS':
                    'required': [
                        'PARTICIPANT|UNIQUE_KEY',
                        'DIAGNOSIS|NAME'
                    ],
                    'optional': [
                        'DIAGNOSIS|EVENT_AGE_IN_DAYS'
                    ]
            }

        If we want to make the unique key for DIAGNOSIS, then at a minimum the
        required columns (PARTICIPANT|ID, DIAGNOSIS|NAME) must be present in
        the DataFrame. If any of the optional columns are also present, they
        will be used to make the unique key too.

        :param concept_name: a string and the name of the concept for which a
        unique key will be made
        :param df: a Pandas DataFrame
        :param unique_key_cols: the output list of columns needed to build the
        unique key column for a concept.
        :param required_cols: the required subset of columns needed to build
        the unique key column for a concept.
        :param optional_cols: the additional columns that can be
        used in the construction of the unique key if they are present
        """

        # Get the cols needed to make a unique key for this concept
        key_comp = deepcopy(unique_key_composition.get(concept_name))

        # If key cols don't exist for a concept, then we have made a dev
        # error in concept_schema.py
        assert key_comp, (
            'Unique key composition not defined in concept '
            f'schema for concept {concept_name}!'
        )

        # If unique key col for this concept already exists return that
        unique_key_col = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
        if unique_key_col in df.columns:
            unique_key_cols.append(unique_key_col)
            required_cols.add(unique_key_col)
            return

        required = key_comp.pop('required', [])
        optional = key_comp.pop('optional', [])
        key_cols = required + optional

        # Expand any unique keys into their basic components
        for key_col in key_cols:
            if concept_attr_from(key_col) == UNIQUE_ID_ATTR:
                # The col is a unique key so recurse
                self._unique_key_cols(
                    concept_from(key_col),
                    df, unique_key_composition,
                    unique_key_cols, required_cols, optional_cols
                )
            else:
                # Add to list of cols needed to make unique key
                unique_key_cols.append(key_col)
                if key_col in required:
                    required_cols.add(key_col)
                elif key_col in optional:
                    optional_cols.add(key_col)
