"""
Table based validation report builder. Produces the following tabular files:

table_reports/
  - validation_results.tsv
  - files_validated.tsv
  - type_counts.tsv

Unpack the validation results JSON into a tabular form where the content is
a little bit less nested and verbose

Extends kf_lib_data_ingest.validation.reporting.base.AbstractReportBuilder
"""
import os
from collections import defaultdict

import pandas

from kf_lib_data_ingest.validation.reporting.base import AbstractReportBuilder

RESULTS_FILENAME = "validation_results.tsv"
TYPE_COUNTS_FILENAME = "type_counts.tsv"
FILES_VALIDATED_FILENAME = "files_validated.tsv"


class TableReportBuilder(AbstractReportBuilder):
    def __init__(self, output_dir=None, setup_logger=False):
        """
        Constructor

        See AbstractReportBuilder.__init__
        """
        super().__init__(output_dir=output_dir, setup_logger=setup_logger)

    def _build(self, results):
        """
        Build a pandas.DataFrame from the validation results

        :param results: See AbstractReportBuilder._build
        :param type: See AbstractReportBuilder._build
        :param title: Report title
        :type title: str
        """
        dfs = []
        # Validation results table
        row_dicts = [
            rd
            for r in results["validation"]
            for rd in self._result_dict_to_df_row(r)
        ]
        dfs.append(
            (
                pandas.DataFrame(row_dicts)[
                    [
                        "type",
                        "result",
                        "description",
                        "errors",
                        "locations",
                        "details",
                    ]
                ],
                RESULTS_FILENAME,
            )
        )
        # Type counts table
        dfs.append(
            (
                pandas.DataFrame(
                    [
                        {"Entity": typ, "Count": count}
                        for typ, count in results["counts"].items()
                    ]
                ),
                TYPE_COUNTS_FILENAME,
            )
        )
        # Files validated
        dfs.append(
            (
                pandas.DataFrame(
                    results["files_validated"], columns={"Files Validated"}
                ),
                FILES_VALIDATED_FILENAME,
            )
        )

        return dfs

    def _write_report(self, dfs):
        """
        Write tabular validation report files to disk:

            table_reports/
              - validation_results.tsv  -> Validation test results
              - files_validated.tsv     -> List of files validated
              - type_counts.tsv         -> Entity counts by type

        :param dfs: list of (pandas.DataFrame, filename)
        :type dfs: list of tuples
        """
        output_dir = os.path.join(self.output_dir, "table_reports")
        os.makedirs(output_dir, exist_ok=True)

        for df, fn in dfs:
            df.to_csv(os.path.join(output_dir, fn), sep="\t", index=False)

        return output_dir

    def _result_dict_to_df_row(self, result):
        """
        Unpack verbose, nested validation result dict into a simpler dict
        suitable for a pandas.DataFrame row

        :param result: See sample_validation_results.py
        :type result: dict
        """
        row_dicts = []
        # Successful tests or tests that did not run
        if not result["errors"]:
            row_dicts.append(
                {
                    "errors": None,
                    "locations": None,
                }
            )
        # -- Failed tests --
        elif result["type"] == "count":
            row_dicts.append(
                {
                    "errors": result["errors"],
                    "locations": None,
                }
            )
        elif result["type"] == "attribute":
            for filepath, invalid_values in result["errors"].items():
                row_dicts.append(
                    {
                        "errors": set(invalid_values),
                        "locations": filepath,
                    }
                )
        elif result["type"] == "relationship":
            for error in result["errors"]:
                # Errors
                from_type, from_val = error["from"]
                details = {from_val: from_type}
                to_nodes = []
                for to_node in error["to"]:
                    to_type, to_val = to_node
                    to_nodes.append(to_val)
                    details[to_val] = to_type
                formatted_errors = (from_val, to_nodes)

                # Locations
                formatted_locations = defaultdict(set)
                for (typ, val), files in error["locations"].items():
                    for f in files:
                        formatted_locations[f].add(f'`{val}`')

                row_dicts.append(
                    {
                        "errors": formatted_errors,
                        "locations": dict(formatted_locations),
                        "details": details,
                    }
                )

        for rd in row_dicts:
            rd.update(
                {
                    "type": result["type"],
                    "description": result["description"],
                    "result": self._result_code(result),
                    "details": rd.get("details"),
                }
            )

        return row_dicts
