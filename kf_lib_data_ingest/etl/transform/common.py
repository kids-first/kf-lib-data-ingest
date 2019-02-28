"""
For transform functionality that will be used by multiple
modules in the kf_lib_data_ingest.etl.transform package
"""
import logging
from copy import deepcopy

from kf_lib_data_ingest.common.concept_schema import (
    concept_from,
    concept_attr_from,
    unique_key_composition as DEFAULT_KEY_COMP,
    UNIQUE_ID_ATTR,
    DELIMITER
)
VALUE_DELIMITER = '-'

logger = logging.getLogger(__name__)


def insert_unique_keys(df_dict, unique_key_composition=DEFAULT_KEY_COMP):
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
    the ingest pipeline saves a record of the created instance's ID from the
    target service and the instance's UNIQUE_KEY. These records are stored
    in the ID cache.

    The ID cache is used for determining whether to create or update an
    entity in the target service each time the ingest pipeline runs.
    When ingest pipeline runs again for the same dataset, it will lookup the
    concept instance's UNIQUE_KEY in the ID cache to see if there is an
    existing target service ID for the instance. If there is, an update will
    be performed. If not, a new instance will be created.

    Construction
    ------------
    In most cases, the UNIQUE_KEY simply equates to the identifier
    (i.e. CONCEPT.PARTICIPANT.ID) assigned to the concept instance in the
    source data by the data provider.

    But some concepts don't have an explicit identifier that was given
    by the data provider. These are usually event type concepts such as
    DIAGNOSIS and PHENOTYPE observation events. For example, `Participant P1
    was diagnosed with Ewing's Sarcoma on date X.` is a DIAGNOSIS event concept

    For these concepts, a unique identifier must somehow be formed using
    existing columns in the data (a.k.a other concept attributes).

    Continuing with the above example, we can uniquely identify the DIAGNOSIS
    event by combining the UNIQUE_KEY of the PARTICIPANT who has been given
    the DIAGNOSIS, the name of the DIAGNOSIS, and the some sort of datetime
    of the event

    Example - DIAGNOSIS.UNIQUE_KEY
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    Composition Formula =

        PARTICIPANT.UNIQUE_KEY + DIAGNOSIS.NAME + DIAGNOSIS.EVENT_AGE_IN_DAYS

    Value of DIAGNOSIS.UNIQUE_KEY =

        P1-Ewings-Sarcoma-500

    The rules for composing UNIQUE_KEYs are defined in `unique_key_composition`
    input parameter, and if not supplied, the default will be loaded from
    kf_lib_data_ingest.common.concept_schema.

    :param df_dict: a dict of Pandas DataFrames. A key is an extract config
    URL and a value is a tuple of (source file url, DataFrame).
    :param unique_key_composition: a dict where a key is a standard concept
    string and a value is a list of required columns needed to compose
    a unique key for the concept
    :returns df_dict: a modified version of the input, with UNIQUE_KEY columns
    added to each DataFrame
    """
    for extract_config_url, (source_file_url, df) in df_dict.items():
        # Insert unique key columns
        df = _add_unique_key_cols(df, unique_key_composition)

        # If no unique key columns are present raise an error.
        # This means that the dataframe does not have anything to uniquely
        # identify concepts in the data. In the case of auto transform
        # this means no ConceptNodes can be created and inserted into the
        # ConceptGraph.
        is_any_unique_keys = any([concept_attr_from(col) == UNIQUE_ID_ATTR
                                  for col in df.columns])
        if not is_any_unique_keys:
            raise ValueError(
                'No unique keys were created for table! There must '
                'be at least 1 unique key column in a table. Zero unique keys '
                'in a table means there is no way to identify any concept '
                f'instances. Source of error is {extract_config_url} : '
                f'{source_file_url}'
            )


def _add_unique_key_cols(df, unique_key_composition):
    """
    Construct and insert UNIQUE_KEY columns for each concept present in the
    mapped df. Only do this if there isn't already an existing UNIQUE_KEY
    column for a particular concept.

    A UNIQUE_KEY column for a concept will only be added if all of the
    columns required to compose the UNIQUE_KEY for that concept exist in
    the DataFrame.

    The rules for composition are defined in unique_key_composition.
    See kf_lib_data_ingest.common.concept_schema._create_unique_key_composition
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
        _unique_key_cols(concept_name,
                         df, unique_key_composition,
                         unique_key_cols, required, optional)

        # Missing required column needed to make the unique key
        missing_req_cols = [col for col in required
                            if col not in df.columns]
        if len(missing_req_cols) > 0:
            logger.debug(
                f'Could not create unique key for {concept_name}. '
                f'Missing required columns {missing_req_cols} needed to '
                'create the key')
            continue

        # Insert unique key column for the concept
        unique_key_col = f'{concept_name}{DELIMITER}{UNIQUE_ID_ATTR}'
        df[unique_key_col] = df.apply(
            lambda row: VALUE_DELIMITER.join([str(row[c])
                                              for c in unique_key_cols
                                              if c in df.columns]), axis=1)

    return df


def _unique_key_cols(concept_name, df, unique_key_composition,
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
    required columns (PARTICIPANT|ID, DIAGNOSIS|NAME) must be present in the
    DataFrame. If any of the optional columns are also present, they will be
    used to make the unique key too.

    :param concept_name: a string and the name of the concept for which a
    unique key will be made
    :param df: a Pandas DataFrame
    :param unique_key_cols: the output list of columns needed to build the
    unique key column for a concept.
    :param required_cols: the required subset of columns needed to build the
    unique key column for a concept.
    :param optional_cols: the additional columns that can be
    used in the construction of the unique key if they are present
    """

    # Get the cols needed to make a unique key for this concept
    key_comp = deepcopy(unique_key_composition.get(concept_name))

    # If key cols don't exist for a concept, then we have made a dev
    # error in concept_schema.py
    assert key_comp, ('Unique key composition not defined in concept '
                      f'schema for concept {concept_name}!')

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
            _unique_key_cols(concept_from(key_col),
                             df, unique_key_composition,
                             unique_key_cols, required_cols, optional_cols)
        else:
            # Add to list of cols needed to make unique key
            unique_key_cols.append(key_col)
            if key_col in required:
                required_cols.add(key_col)
            elif key_col in optional:
                optional_cols.add(key_col)
