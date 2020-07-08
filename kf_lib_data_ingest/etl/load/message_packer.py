"""
The MessagePacker class turns Pandas DataFrames of CONCEPT columns returned by
the TransformStage into sendable service payloads. This is used by the Load
Stage to generate sendable payloads.
"""
import logging
from collections import defaultdict
from copy import deepcopy
from pprint import pformat

import pandas

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import (
    DELIMITER,
    UNIQUE_ID_ATTR,
    concept_from,
    is_identifier,
)
from kf_lib_data_ingest.common.misc import convert_to_downcasted_str
from kf_lib_data_ingest.config import DEFAULT_KEY, DEFAULT_TARGET_URL
from kf_lib_data_ingest.network.utils import get_open_api_v2_schema
from kf_lib_data_ingest.target_apis.kids_first import (
    unique_key_composition as DEFAULT_KEY_COMP,
)

VALUE_DELIMITER = "-"


class MessagePacker:
    def __init__(
        self, target_api_config, target_api_url=DEFAULT_TARGET_URL, logger=None
    ):
        """
        :param target_api_config: Config module describing a target api
        :type target_api_config: TargetAPIConfig
        :param target_api_url: API server, defaults to DEFAULT_TARGET_URL
        :type target_api_url: str
        :param logger: a logger instance, defaults to None
        :type logger: logging.Logger, optional
        """
        self.logger = logger or logging.getLogger(type(self).__name__)
        self.target_api_url = target_api_url
        self.target_api_config = target_api_config

    def pack_messages(self, df_dict):
        """
        Main entry method for building messages.

        For each DataFrame in df_dict, convert the DataFrame containing all of
        the mapped source data into a dict of lists of dicts representing lists
        of target concept instances with nulls replaced by acceptable values
        based on the type of property as defined in the target API config.

        :param df_dict: dict of dataframes keyed by name
        :return: lists of message payloads for each payload type
        :rtype: dict of lists of dicts
            (See docstrings for _standard_to_target and _handle_nulls.)
        """
        # Insert unique key columns
        for key, df in df_dict.items():
            self._insert_unique_keys(
                {
                    key
                    + ".tsv": (
                        f"Transform Module Output: {key} df",
                        df_dict[key],
                    )
                }
            )

        target_instances = self._standard_to_target(df_dict)

        target_api_schema = get_open_api_v2_schema(
            self.target_api_url,
            list(target_instances.keys()),
            logger=self.logger,
        )
        if target_api_schema:
            self._handle_nulls(target_instances, target_api_schema)
        else:
            self.logger.warning(
                "Skipping null processing because no target schema was found"
            )

        return target_instances

    def _standard_to_target(self, df_dict):
        """
        For each DataFrame in df_dict, convert the DataFrame containing all of
        the mapped source data into a dict of lists of dicts representing
        represent lists of target concept instances.

        When building instances of a particular target concept type, first
        check to see whether an explicit key for that target concept exists.
        If it does, then build the target instances for this target concept
        using the value for that key, which will be a DataFrame.

        Otherwise, use the DataFrame stored in the value for the default key.
        See kf_lib_data_ingest.config.DEFAULT_KEY for the name of the default
        key.

        For example,

        This:

        {
            'diagnosis': diagnosis_df
            'default': merged_df
        }

        where diagnosis_df looks like:

        | DIAGNOSIS.UNIQUE_KEY | PARTICIPANT.UNIQUE_KEY | DIAGNOSIS.EVENT_AGE_DAYS | DIAGNOSIS.NAME  | # noqa E501
        |----------------------|------------------------|--------------------------|-----------------| # noqa E501
        |    P1-CDH-400        |             P1         |         400              |     CDH         | # noqa E501
        |    P2-CNS-500        |             P2         |         500              |     CNS         | # noqa E501

        and merged_df looks like:

        |     BIOSPECIMEN.UNIQUE_KEY    |  BIOSPECIMEN.ANALYTE  |  PARTICIPANT.UNIQUE_KEY | ... | # noqa E501
        |-------------------------------|-----------------------|-------------------------|-----| # noqa E501
        |            B1                 |            DNA        |           P1            | ... | # noqa E501
        |            B2                 |            RNA        |           P2            | ... | # noqa E501

        Turns into:

        {
            'diagnosis': [
                {
                    'id': 'P1-CDH-400',
                    'properties': {
                         'age_at_event_days': 400,
                         'source_text': 'CDH',
                         ...
                    },
                    links: {
                        'participant_id': 'P1'
                    }
                },
                {
                    'id': 'P2-CNS-500',
                    'properties': {
                         'age_at_event_days': 500,
                         'source_text': 'CNS',
                         ...
                    },
                    links: {
                        'participant_id': 'P2'
                    }
                },
            ],
            'biospecimen': [
                {
                    'id': 'B1',
                    'properties': {
                        'analyte_type': DNA
                         ...
                    },
                    links: {
                        'participant_id': 'P1'
                    }
                },
                {
                    'id': 'B2',
                    'properties': {
                        'analyte_type': RNA
                         ...
                    },
                    links: {
                        'participant_id': 'P2'
                    }
                }
            ],
            ...
        }

        :param df_dict: the output of the user transform function after
        unique keys have been inserted
        :type df_dict: a dict of pandas.DataFrames
        :return: target_instances: dict (keyed by target concept) of lists
        of dicts (target concept instances)
        :rtype: dict
        """
        self.logger.info(
            "Begin transformation from standard concepts "
            "to target concepts ..."
        )

        target_instances = defaultdict(list)
        for (
            target_concept,
            config,
        ) in self.target_api_config.target_concepts.items():

            # Get the df containing data for this target concept
            all_data_df = df_dict.get(target_concept)
            if all_data_df is not None:
                self.logger.info(f"Using {target_concept} DataFrame")
            else:
                all_data_df = df_dict.get(DEFAULT_KEY)

            if all_data_df is not None:
                self.logger.info(f"Using {DEFAULT_KEY} DataFrame")
            else:
                self.logger.warning(
                    f"Cannot build target concept instances for "
                    f"{target_concept}! No DataFrame found in transform "
                    "function output dict."
                )
                continue

            # Unique key for the target concept must exist
            standard_concept = config.get("standard_concept")
            std_concept_ukey = getattr(standard_concept, UNIQUE_ID_ATTR)
            if std_concept_ukey not in all_data_df.columns:
                self.logger.info(
                    "No unique key found in table for target "
                    f"concept: {target_concept}. Skip instance creation"
                )
                continue

            # Drop duplicates using unique key of std concept
            df = all_data_df.drop_duplicates(subset=std_concept_ukey)

            # Build target instances for target_concept (i.e. participant)
            self.logger.info(f"Building {target_concept} concepts ...")
            for _, row in df.iterrows():
                target_instance = {}
                # id
                target_instance["id"] = row[std_concept_ukey]

                # Skip building target instances with null ids
                if not target_instance["id"]:
                    continue

                # endpoint
                target_instance["endpoint"] = config["endpoint"]

                # properties
                target_instance["properties"] = defaultdict()
                for (target_attr, value) in config["properties"].items():
                    if isinstance(value, tuple):
                        std_concept_attr = value[0]
                    else:
                        std_concept_attr = value

                    target_instance["properties"][target_attr] = row.get(
                        std_concept_attr
                    )

                # links
                target_instance["links"] = []
                if "links" in config:
                    for link_dict in config["links"]:
                        unique_k_col = getattr(
                            link_dict["standard_concept"], UNIQUE_ID_ATTR
                        )
                        unique_k_val = row.get(unique_k_col)
                        target_instance["links"].append(
                            {
                                link_dict["target_attribute"]: unique_k_val,
                                "target_concept": link_dict["target_concept"],
                            }
                        )
                target_instances[target_concept].append(target_instance)

            self.logger.info(
                f"Built {len(target_instances[target_concept])} "
                f"{target_concept} concepts"
            )

        return target_instances

    def _handle_nulls(self, target_instances, target_schema):
        """
        Convert null property values in `target_instances` to acceptable values
        based on the type of property as defined in target schema.

        See kf_lib_data_ingest.network.utils.get_open_api_v2_schema for
        expected format of target_schema

        `target_instances` is a dict keyed by the `target_concepts` defined in
        this `target_api_config.target_concepts`. The values are lists of
        dicts, where a dict in the list takes on the same form as the dicts in
        `target_api_config.target_concepts`.

        For example, `target_instances` might look like:

        {
            'participant': [
                {
                    "endpoint": "/participants",
                    "id": "1",
                    "links": [
                        {
                            "family_id": "f1",
                            "target_concept": "family"
                        },
                        {
                            "study_id": null,
                            "target_concept": "study"
                        }
                    ],
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
                    "links": [
                        {
                            "family_id": "f1",
                            "target_concept": "family"
                        },
                        {
                            "study_id": null,
                            "target_concept": "study"
                        }
                    ]
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
        :return: target_instances: Updated version of the input
        """
        version = target_schema.get("version")
        schemas = target_schema.get("definitions")
        self.logger.info(
            f"Do null processing using target schema "
            f'{target_schema.get("target_service")}, '
            f"version {pformat(version)}"
        )

        for target_concept, list_of_instances in target_instances.items():
            # Get schema for target concept
            schema = schemas.get(target_concept)
            if not schema:
                self.logger.info(
                    f"Skip handle nulls for {target_concept}. No schema "
                    "was found."
                )
                continue
            # Convert nulls
            conversions = {}
            for i, instance in enumerate(list_of_instances):
                for attr, value in instance["properties"].items():
                    if value is not None:
                        continue
                    if attr in conversions:
                        instance["properties"][attr] = conversions[attr]
                        continue

                    property_def = schema["properties"].get(attr)
                    mapped_value = None
                    if not property_def:
                        self.logger.warning(
                            "No property definition found for "
                            f"{target_concept}.{attr} in target schema "
                            "This property may not exist "
                            "anymore in the target service."
                        )
                    else:
                        if property_def["type"] == "string":
                            mapped_value = constants.COMMON.NOT_REPORTED
                            # Note - the dataservice should tell us if the
                            # format of the string is `date-time`, then we
                            # could do:
                            #   property_def.get('format') == 'date-time'.
                            # Right now it doesn't do this.
                            # That's what we should use to determine what null
                            # value to use. For now, we're just using the name
                            # of the attribute
                            if "date" in attr:
                                mapped_value = None
                        else:
                            mapped_value = None

                    self.logger.debug(
                        f"Mapping {target_concept}.{attr} value {value} to "
                        f"{mapped_value}"
                    )
                    conversions[attr] = mapped_value

                    instance["properties"][attr] = mapped_value

    # TODO: Eliminate these complex Unique Key generation functions.
    # Unique key composition code can be completely one-lined by
    # producing them on the fly during load instead of all in advance. Since
    # requisite components must be loaded first, there's no need for recursive
    # discovery because target service IDs will necessarily already exist. See
    # e.g. the simple calls to self.key in Avi's old loading code
    # https://github.com/kids-first/kf-study-imports/blob/8d0aabf0200678419d2d6dd69bb081d678bfc04c/Avi/load.py#L348
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
        :return: df_dict: a modified version of the input, with UNIQUE_KEY
        columns added to each DataFrame
        """
        self.logger.info("Begin unique key creation for standard concepts ...")
        for reference, (df_name, df) in df_dict.items():
            # Insert unique key columns
            self.logger.info(f"Creating unique keys for {df_name}...")
            df = self._add_unique_key_cols(df, unique_key_composition)

            # If no unique key columns are present raise an error.
            # This means that the dataframe does not have anything to uniquely
            # identify concepts in the data.
            is_any_unique_keys = any(is_identifier(col) for col in df.columns)
            if not is_any_unique_keys:
                raise ValueError(
                    "No unique keys were created for table! There must "
                    "be at least 1 unique key column in a table. Zero unique "
                    "keys in a table means there is no way to identify any "
                    "concept instances. Source of error is "
                    f"{reference} : {df_name}"
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
        :return: modified version of input DataFrame with new UNIQUE_KEY
        columns inserted for each concept present in the input DataFrame.
        :rtype: DataFrame
        """
        # Iterate over all concepts and try to insert a unique key column
        # for each concept
        for concept_name in unique_key_composition:
            # Determine the cols needed to compose a unique key for the concept
            unique_key_cols = []
            required = set()
            optional = set()
            self.logger.debug(f"Creating unique key for {concept_name}")
            self._unique_key_cols(
                concept_name,
                df,
                unique_key_composition,
                unique_key_cols,
                required,
                optional,
            )

            # Missing required column needed to make the unique key
            missing_req_cols = [
                col for col in required if col not in df.columns
            ]
            if len(missing_req_cols) > 0:
                self.logger.debug(
                    f"Could not create unique key for {concept_name}. "
                    f"Missing required columns {missing_req_cols} needed to "
                    "create the key"
                )
                continue

            # Insert unique key column for the concept
            unique_key_col = f"{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}"

            # Make unique key string out of the individual col values
            # Any required cols must be non-null
            def make_unique_key_value(row):
                values = []
                for c in unique_key_cols:
                    # Col is required to make unique key but is null
                    if c in required and pandas.isnull(row[c]):
                        return None

                    # Optional col is not in df,
                    # fill in with constants.COMMON.NOT_REPORTED
                    if (c in optional) and c not in row:
                        row[c] = constants.COMMON.NOT_REPORTED

                    # Optional cols whose values are null will be converted
                    # to constants.COMMON.NOT_REPORTED
                    values.append(
                        convert_to_downcasted_str(
                            row.get(c),
                            replace_na=True,
                            na=constants.COMMON.NOT_REPORTED,
                        )
                    )

                return VALUE_DELIMITER.join(values)

            df[unique_key_col] = df.apply(
                lambda row: make_unique_key_value(row), axis=1
            )
            self.logger.debug(f"Done creating unique key for {concept_name}")

        unique_concepts_found = [
            col for col in df.columns if col.endswith(UNIQUE_ID_ATTR)
        ]
        self.logger.info(
            f"Found {len(unique_concepts_found)} standard concepts in "
            f"table:\n{pformat(unique_concepts_found)}"
        )

        return df

    def _unique_key_cols(
        self,
        concept_name,
        df,
        unique_key_composition,
        unique_key_cols,
        required_cols,
        optional_cols,
    ):
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
        self.logger.debug(f"Composing unique key columns for {concept_name}")
        # Get the cols needed to make a unique key for this concept
        key_comp = deepcopy(unique_key_composition.get(concept_name))

        # If key cols don't exist for a concept, then we have made a dev
        # error in concept_schema.py
        assert key_comp, (
            "Unique key composition not defined in concept "
            f"schema for concept {concept_name}!"
        )

        # If unique key col for this concept already exists return that
        unique_key_col = f"{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}"
        if unique_key_col in df.columns:
            unique_key_cols.append(unique_key_col)
            required_cols.add(unique_key_col)
            return

        required = key_comp.pop("required", [])
        optional = key_comp.pop("optional", [])
        key_cols = required + optional

        # Expand any unique keys into their basic components
        for key_col in key_cols:
            if is_identifier(key_col):
                # The col is a unique key so recurse
                self._unique_key_cols(
                    concept_from(key_col),
                    df,
                    unique_key_composition,
                    unique_key_cols,
                    required_cols,
                    optional_cols,
                )
            else:
                # Add to list of cols needed to make unique key
                unique_key_cols.append(key_col)
                if key_col in required:
                    required_cols.add(key_col)
                elif key_col in optional:
                    optional_cols.add(key_col)
        self.logger.debug(f"Done composing columns for {concept_name}")
