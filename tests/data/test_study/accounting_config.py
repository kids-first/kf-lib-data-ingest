"""
Auto-generated accounting config module

Replace the contents of test_parameters

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing the accounting config.
"""

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.accounting import test

# **** Define custom test functions here **** #


def proband_count(df_dict, **inputs):
    # do something
    pass


test_parameters = [

    # **** Auto-generated default tests **** #
    {
        'function': test.unique_concept_count,
        'inputs': {
            'concept': CONCEPT.PARTICIPANT.ID,
            'expected': 100,  # we expect exactly 100 participants
            'test': '=='
        },
        'enable': True
    },
    {
        'function': test.unique_concept_count,
        'inputs': {
            'concept': CONCEPT.BIOSPECIMEN.ID,
            'expected': 100,  # we expect exactly 100 biospecimens
            'test': '=='
        },
        'enable': True
    },
    {
        'function': test.a_has_b,
        'inputs': {
            'a': CONCEPT.BIOSPECIMEN.ID,
            'b': CONCEPT.PARTICIPANT.ID,
            'expected': 1,  # we expect each biospecimen to have exactly 1 biospecimen
            'test': '=='
        },
        'enable': True
    },
    # **** Put your additional tests here **** #
    {
        'function': test.unique_family_group_count,
        'inputs': {
            'concept': CONCEPT.FAMILY.ID,
            'group_size': 3,
            'expected': 30,  # we expect exactly 30 trios
            'test': '=='
        },
        'enable': True
    },
    {
        'function': test.a_has_b,
        'inputs': {
            'a': CONCEPT.PARTICIPANT.ID,
            'b': CONCEPT.BIOSPECIMEN.ID,
            'expected': 1,  # we expect each participant to have at least 1 biospecimen
            'test': '>='
        },
        'enable': True
    },
    {
        'function': proband_count,  # count num of probands
        'inputs': {
            'concept': CONCEPT.PARTICIPANT.ID,
            'expected': 50,
            'test': '=='
        },
        'enable': True
    }
]
