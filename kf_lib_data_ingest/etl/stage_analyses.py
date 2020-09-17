from pprint import pformat

import pandas
from tabulate import tabulate

from kf_lib_data_ingest.common.concept_schema import str_to_CONCEPT

SEP = "-----"
INFO = "⚠️"
NO = "❌"
YES = "✅"
NO_EXPECTED_COUNTS = f"{INFO} No expected counts registered."


def check_counts(discovery_sources, expected_counts):
    """
    Verify that we found as many unique values of each attribute as we expected
    to find.

    :param discovery_sources: 'sources' subcomponent of the structure returned
     from IngestStage._postrun_concept_discovery
    :param expected_counts: dict mapping concept keys to their expected counts

    :return: all checks passed (bool), output message to emit/log (str)
    """
    uniques = {
        key: len(unique_vals) for key, unique_vals in discovery_sources.items()
    }
    uniques_without_na = {
        key: len([v for v in unique_vals if v is not None])
        for key, unique_vals in discovery_sources.items()
    }
    messages = ["UNIQUE COUNTS:\n" + pformat(uniques)]
    passed = True

    if not expected_counts:
        messages.append(NO_EXPECTED_COUNTS)
        passed = True  # Pass if we have no expectations
    else:
        checks = pandas.DataFrame(columns=["Key", "Expected", "Found", "Equal"])
        for key, expected in expected_counts.items():

            # Accept both concepts and attributes, but automatically
            # translate lone concepts as meaning id or else unique_key if
            # one of those was discovered in the data.
            if key not in discovery_sources:
                if key in str_to_CONCEPT:
                    key = str_to_CONCEPT[key]
                if hasattr(key, "ID") and key.ID in discovery_sources:
                    key = key.ID
                elif (
                    hasattr(key, "UNIQUE_KEY")
                    and key.UNIQUE_KEY in discovery_sources
                ):
                    key = key.UNIQUE_KEY

            found = uniques_without_na.get(key)
            if expected == found:
                flag = YES
            else:
                passed = False
                flag = NO
            checks = checks.append(
                {
                    "Key": key,
                    "Expected": expected,
                    "Found": found,
                    "Equal": flag,
                },
                ignore_index=True,
            )

        if passed:
            messages.append(f"{YES} All counts are as expected.")
        else:
            messages.append(f"{NO} Counts are not all as expected.")

        messages.append(
            "EXPECTED COUNT CHECKS:\n"
            + tabulate(checks, headers="keys", showindex=False, tablefmt="psql")
        )

    return passed, messages


def compare_counts(
    name_one, discovery_sources_one, name_two, discovery_sources_two
):
    title = "COMPARING COUNTS"
    messages = [title + "\n" + "=" * len(title)]
    passed = True

    one_keys = discovery_sources_one.keys()
    two_keys = discovery_sources_two.keys()

    comparison_df, diff_num = _compare(name_one, one_keys, name_two, two_keys)

    if diff_num != 0:
        msg = f"{NO} Column names not equal between {name_one} and {name_two}"
        messages.extend(_format(msg, comparison_df))
        passed = False
    else:
        msg = f"{YES} Column names are equal between {name_one} and {name_two}"
        messages.append(msg)
        messages.append(SEP)

    good_msg_ag = []
    bad_msg_ag = []
    for k in one_keys | two_keys:
        one_k_keys = discovery_sources_one.get(k, {}).keys()
        two_k_keys = discovery_sources_two.get(k, {}).keys()

        comparison_df, diff_num = _compare(
            name_one, one_k_keys, name_two, two_k_keys
        )

        if diff_num != 0:
            msg = (
                f"{NO} Column values for {k} not equal between {name_one} and"
                f" {name_two}\n"
                f"Number of different values = {diff_num}"
            )
            bad_msg_ag.extend(_format(msg, comparison_df))
            passed = False
        else:
            good_msg_ag.append(
                f"{YES} Column values for {k} are equal between {name_one} and"
                f" {name_two}"
            )
    messages.append("\n".join(good_msg_ag))
    messages.append(SEP)
    messages.extend(bad_msg_ag)

    return passed, messages


def _compare(name_one, list_one, name_two, list_two):
    indicator = "In Both"
    one = pandas.DataFrame({name_one: [v for v in list_one if v is not None]})
    two = pandas.DataFrame({name_two: [v for v in list_two if v is not None]})

    comparison_df = pandas.merge(
        one,
        two,
        left_on=name_one,
        right_on=name_two,
        how="outer",
        indicator=indicator,
    )
    comparison_df = comparison_df.astype(object).fillna("")
    comparison_df = comparison_df[comparison_df[indicator] != "both"][
        [name_one, name_two]
    ]
    diff_num = comparison_df.shape[0]
    comparison_df.columns = "In " + comparison_df.columns
    return comparison_df, diff_num


def _format(pre_msg, comparison_df):
    return [
        pre_msg,
        tabulate(
            comparison_df,
            headers=comparison_df.columns.tolist(),
            showindex=False,
            tablefmt="psql",
        ),
        SEP,
    ]
