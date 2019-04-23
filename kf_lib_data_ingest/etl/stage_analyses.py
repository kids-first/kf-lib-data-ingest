from pprint import pformat

import pandas
from tabulate import tabulate

from kf_lib_data_ingest.common.concept_schema import str_to_CONCEPT
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage


def check_counts(discovery_sources, expected_counts, logger):
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

    logger.info('UNIQUE COUNTS:\n' + pformat(uniques))

    # check expected counts

    passed = True

    if not expected_counts:
        logger.info("No expected counts registered. ❌")
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
                    key = str_to_CONCEPT[key]
                if key.ID in discovery_sources:
                    key = key.ID
                elif key.UNIQUE_KEY in discovery_sources:
                    key = key.UNIQUE_KEY

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
        logger.info(
            'EXPECTED COUNT CHECKS\n' +
            tabulate(
                checks,
                headers='keys', showindex=False, tablefmt='psql'
            )
        )

    return passed


def compare_counts(
    name_one, discovery_sources_one, name_two, discovery_sources_two, logger
):
    logger.info(f'COMPARING COUNTS')
    passed = True

    setA = set(discovery_sources_one.keys())
    setB = set(discovery_sources_two.keys())
    diff = setA ^ setB
    if diff:
        logger.info(
            f'❌ Column keys not equal between {name_one} and '
            f'{name_two}'
        )
        logger.debug(
            f'{name_one} {setA}\n'
            'vs\n'
            f'{name_two} {setB}\n'
            'Difference is:\n' +
            str(diff)
        )
        passed = False

    for k, v in discovery_sources_one.items():
        setA = set(v.keys())
        setB = set(discovery_sources_two[k].keys())
        diff = setA ^ setB
        if diff:
            logger.info(
                f'❌ Column values for {k} not equal between {name_one} and '
                f'{name_two}  ( # = {len(diff)})'
            )
            logger.info(f'Number of different values = {len(diff)}')
            logger.debug(
                f'{name_one} {setA}\n'
                'vs\n'
                f'{name_two} {setB}\n'
                'Difference is:\n' +
                str(diff)
            )
            passed = False

    return passed
