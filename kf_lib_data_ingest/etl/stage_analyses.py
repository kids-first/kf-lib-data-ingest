from pprint import pformat

import pandas
from tabulate import tabulate

from kf_lib_data_ingest.common.concept_schema import str_to_CONCEPT
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage


def check_counts(discovery_sources, expected_counts):
    """
    Verify that we found as many unique values of each attribute as we expected
    to find.

    :param discovery_sources: 'sources' subcomponent of the structure returned
     from IngestStage._postrun_concept_discovery_postrun_concept_discovery
    :param expected_counts: dict mapping concept keys to their expected counts

    :return: all checks passed (bool), output message to emit/log (str)
    """
    uniques = {
        key: len(unique_vals) for key, unique_vals in discovery_sources.items()
    }

    # display unique counts

    message = ['UNIQUE COUNTS']
    message.append(
        pformat(uniques)
    )
    message.append('')

    # check expected counts

    passed = True
    message.append('EXPECTED COUNT CHECKS')

    if not expected_counts:
        message.append("No expected counts registered. ❌")
        passed = True  # Pass if we have no expectations
    else:
        checks = pandas.DataFrame(
            columns=['key', 'expected', 'found', 'equal']
        )
        for key, expected in expected_counts.items():

            # Accept both concepts and attributes, but automatically
            # translate lone concepts as meaning id or else unique_key if
            # one of those was discovered in the data.
            if key not in discovery_sources:
                if key in str_to_CONCEPT:
                    concept = str_to_CONCEPT[key]
                    if concept.ID in discovery_sources:
                        key = concept.ID
                    elif concept.UNIQUE_KEY in discovery_sources:
                        key = concept.UNIQUE_KEY

            found = uniques.get(key)
            if expected == found:
                passmark = '✅'
            else:
                passed = False
                passmark = '❌'
            checks = checks.append(
                {
                    'key': key, 'expected': expected, 'found': found,
                    'equal': passmark
                },
                ignore_index=True
            )
        message.append(
            tabulate(
                checks,
                headers='keys', showindex=False, tablefmt='psql'
            )
        )

    return passed, '\n'.join(message)


def compare_counts(discovery_sources_one, discovery_sources_two):
    False, "Compare Counts NYI"
