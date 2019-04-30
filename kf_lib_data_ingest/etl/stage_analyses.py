from pprint import pformat

import pandas
from tabulate import tabulate

from kf_lib_data_ingest.common.concept_schema import str_to_CONCEPT


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
        if pandas.notnull(key)
    }
    uniques_without_na = {
        key: len([v for v in unique_vals if v is not None])
        for key, unique_vals in discovery_sources.items()
    }
    messages = ['UNIQUE COUNTS:\n' + pformat(uniques)]
    passed = True

    # check expected counts
    if not expected_counts:
        messages.append("No expected counts registered. ⚠️")
        passed = True  # Pass if we have no expectations
    else:
        checks = pandas.DataFrame(
            columns=['Key', 'Expected', 'Found', 'Equal']
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

            found = uniques_without_na.get(key)
            if expected == found:
                flag = '✅'
            else:
                passed = False
                flag = '❌'
            checks = checks.append(
                {
                    'Key': key, 'Expected': expected, 'Found': found,
                    'Equal': flag
                },
                ignore_index=True
            )
        messages.append(
            'EXPECTED COUNT CHECKS:\n' +
            tabulate(
                checks,
                headers='keys', showindex=False, tablefmt='psql',
                stralign='center'
            )
        )

    return passed, messages


def compare_counts(
    name_one, discovery_sources_one, name_two, discovery_sources_two
):
    title = 'COMPARING COUNTS'
    messages = [title + '\n' + '='*len(title)]
    passed = True

    col_list_one = list(discovery_sources_one.keys())
    col_list_two = list(discovery_sources_two.keys())
    comparison_df, diff_count = _compare(name_one, col_list_one,
                                         name_two, col_list_two)

    if diff_count != 0:
        msg = f'❌ Column names not equal between {name_one} and {name_two}'
        messages.extend(_format(msg, comparison_df))
        passed = False

    for k in set(col_list_one + col_list_two):
        comparison_df, diff_count = _compare(
            name_one, [key for key in discovery_sources_one.get(k, {}).keys()
                       if pandas.notnull(key)],
            name_two, [key for key in discovery_sources_two.get(k, {}).keys()
                       if pandas.notnull(key)]
        )

        if diff_count != 0:
            msg = (
                f'❌ Column values for {k} not equal between {name_one} and '
                f'{name_two}\n'
                f'Number of different unique values = {diff_count}'
            )
            messages.extend(_format(msg, comparison_df))
            passed = False

    return passed, messages


def _compare(
    name_one, list_one, name_two, list_two
):
    indicator = 'Match'
    one = pandas.DataFrame({name_one: list_one})
    two = pandas.DataFrame({name_two: list_two})
    comparison_df = pandas.merge(one, two,
                                 left_on=name_one,
                                 right_on=name_two,
                                 how='outer', indicator=indicator)
    diff_count = comparison_df[comparison_df[indicator] != 'both'].shape[0]

    comparison_df[indicator].replace({
        'left_only': '❌',
        'right_only': '❌',
        'both': '✅'
    }, inplace=True)

    return comparison_df, diff_count


def _format(pre_msg, comparison_df):
    return [
        pre_msg,
        tabulate(
            comparison_df, headers=comparison_df.columns.tolist(),
            showindex=False, tablefmt='psql', stralign='center'
        ),
        '-----'
    ]
