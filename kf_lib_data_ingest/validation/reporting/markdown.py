"""
Markdown validation report builder
Produces a human friendly markdown based validation report

Extends kf_lib_data_ingest.validation.reporting.base.AbstractReportBuilder
"""

import os
import re
from collections import defaultdict

import pandas
from kf_lib_data_ingest.validation.reporting.base import (
    FAILED,
    NA,
    PASSED,
    RESULT_TO_EMOJI,
    RESULTS_FILENAME,
    AbstractReportBuilder,
)

DEFAULT_REPORT_TITLE = "📓 Data Validation Report"
REPLACE_PIPE = "."
REL_TEST = "relationship"
GAP_TEST = "gaps"
ATTR_TEST = "attribute"
COUNT_TEST = "count"
ROW_LIMIT = 20


def md_clean(val, not_inside_backticks=False):
    if isinstance(val, str):
        if not_inside_backticks:
            backtick_groups = re.split("`", val)
            if len(backtick_groups) == 1:
                return md_clean(val)
            for i, v in list(enumerate(backtick_groups))[::2]:
                backtick_groups[i] = md_clean(v)
            return "`".join(backtick_groups)
        else:
            return re.sub(r"([\\\`\*\_\{\}\[\]\(\)\#\+\-\.\!])", r"\\\1", val)
    elif isinstance(val, list):
        return [md_clean(v, not_inside_backticks) for v in val]
    else:
        return val


class MarkdownReportBuilder(AbstractReportBuilder):
    def __init__(self, output_dir=None, setup_logger=False):
        """
        Constructor

        See AbstractReportBuilder.__init__
        """
        super().__init__(output_dir=output_dir, setup_logger=setup_logger)

    def _build(self, results, title=DEFAULT_REPORT_TITLE, exclude_tests=None):
        """
        Build markdown content for validation report

        :param results: See AbstractReportBuilder._build
        :param type: See AbstractReportBuilder._build
        :param title: Report title
        :type title: str
        """
        exclude_tests = exclude_tests or []
        self.counts = results["counts"]
        self.files_validated = results["files_validated"]
        self.results = results["validation"]

        output = []
        output.append(f"# {title}")
        output.append("## 📂 Files Validated")
        output.append(self._files_md(results))
        output.append("\n## #️⃣ Counts")
        output.append(self._counts_md(results))

        tests = [REL_TEST, GAP_TEST, ATTR_TEST, COUNT_TEST]
        for test_type in tests:
            if test_type in exclude_tests:
                continue
            output.append(f"\n## 🚦 {test_type.title()} Tests")
            output.append(self._tests_section_md(results, test_type))
        output = "\n".join(output)

        return output

    def _write_report(self, content):
        """
        Write markdown validation report file to disk
        """
        output_path = os.path.join(self.output_dir, RESULTS_FILENAME + ".md")
        with open(output_path, "w") as md_file:
            md_file.write(content)
        return output_path

    def _test_header(self, result_dict):
        """
        Create validation test header from a test result dict

        :param result_dict: See sample_validation_results.py
        :type results: dict

        :returns: markdown formatted str
        """
        header = md_clean(
            re.sub(
                r"'(.*?)'",
                r'`"\1"`',
                (
                    f"{RESULT_TO_EMOJI.get(self._result_code(result_dict))} "
                    f'{result_dict["description"]}'
                ),
            ).replace("|", REPLACE_PIPE),
            True,
        )

        # Make test header stand out for tests that ran
        if result_dict["is_applicable"]:
            header = f"#### {header}"

        return header

    def _tests_section_md(self, results, test_type):
        """
        Create markdown section containing test results for a given
        test type: [COUNT_TEST | ATTR_TEST | REL_TEST | GAP_TEST]

        :param results: See sample_validation_results.py for `results` format
        :type results: list of dicts
        :param test_type: Type of validation test
        (e.g. attribute, relationship, count)
        :type test_type: str

        :returns: markdown formatted str
        """
        output = []
        results = results["validation"]

        # Build markdown for each test section
        test_markdowns = defaultdict(list)
        for r in results:
            if r["type"] != test_type:
                continue

            self.logger.debug(f"Building results for {r['description']}")
            # Build markdown for each test result
            test_markdowns[self._result_code(r)].append(
                self._result_to_md(r, test_type)
            )

        # Add test section summary
        test_order_by_outcome = [FAILED, PASSED, NA]
        summary_md = self._df_to_markdown(
            pandas.DataFrame(
                [
                    {
                        "Result": f"**{RESULT_TO_EMOJI.get(error_code)} "
                        f"{error_code.title()}**",
                        "# of Tests": len(test_markdowns.get(error_code, [])),
                    }
                    for error_code in test_order_by_outcome
                ]
            )
        )
        output.append("### Result Summary")
        output.append(summary_md)

        # Add test markdowns
        for result_code in test_order_by_outcome:
            tm = test_markdowns.get(result_code, [])
            output.extend(tm)

        return "\n".join(output)

    def _result_to_md(self, rd, test_type):
        """
        Helper method for _tests_section_md
        Convert single test result dict into markdown formatted text
        """

        def _style(val, with_quotes=False):
            """
            Style string val to make it stand out in a markdown doc
            Replace | char with .
            """
            valstr = str(val).replace("|", REPLACE_PIPE)
            if with_quotes:
                return f'`"{valstr}"`'
            else:
                return f"`{valstr}`"

        def tuple_to_str(t):
            """
            Turn (type, value) concept into a string (e.g. `PARTICIPANT.ID`)
            """
            return _style(REPLACE_PIPE.join(t))

        def _format_errors(result_dict, include_node_type=False):
            """
            Transform error dicts into formatted error strings
            Return list of error row dicts for DataFrame
            """
            errors = []
            for e in result_dict["errors"]:
                prefix = f"{tuple_to_str(e['from'])} is linked to "
                if e["to"]:
                    suffix = []
                    for to_node in e["to"]:
                        if include_node_type:
                            suffix.append(tuple_to_str(to_node))
                        else:
                            suffix.append(_style(to_node[1]))

                else:
                    suffix = f"0 {_style(rd['inputs']['to'])}"

                errors.append({"Errors": md_clean(f"{prefix} {suffix}", True)})
            return errors

        def _format_locations(result_dict, include_node_type=False):
            """
            Transform location dicts into formatted location strings
            Return list of location row dicts for DataFrame
            """
            # -- File locations --
            # Organize error values by file they were found in
            locs = defaultdict(set)
            for e in result_dict["errors"]:
                for (typ, val), files in e["locations"].items():
                    for f in files:
                        if include_node_type:
                            val = tuple_to_str((typ, val))
                        else:
                            val = _style(val)
                        locs[f].add(md_clean(val, True))

            loc_rows = []
            for loc, vals in locs.items():
                # Rollup locations - if number of error values in a file ==
                # number of total errors for the test, then don't list out
                # every error value in that file
                if len(vals) == len(rd["errors"]):
                    val_str = "This file contains all errors in the **Errors** table above."
                else:
                    val_str = ",".join(sorted(vals))
                loc_rows.append(
                    {
                        "Locations": md_clean(os.path.basename(loc)),
                        "Values": val_str,
                    }
                )
            return loc_rows

        # Add test header - [result emoji] [test description]
        test_markdown = []
        test_markdown.append("")
        test_markdown.append(self._test_header(rd))
        test_markdown.append("")

        # If test has no errors, just return test header
        if not rd["errors"]:
            return "\n".join(test_markdown)

        # Add test errors and locations
        if test_type == COUNT_TEST:
            test_markdown.append(
                f'Found: {_style(rd["errors"]["found"])} '
                f'but expected: {_style(rd["errors"]["expected"])}'
            )

        elif test_type == ATTR_TEST:
            test_markdown.append(
                self._df_to_markdown(
                    pandas.DataFrame(
                        [
                            {
                                "Locations": md_clean(
                                    os.path.basename(file_path)
                                ),
                                "Bad Values": md_clean(
                                    ",".join(
                                        sorted(
                                            [_style(v, True) for v in bad_vals]
                                        )
                                    ),
                                    True,
                                ),
                            }
                            for file_path, bad_vals in rd["errors"].items()
                        ]
                    )
                )
            )
        elif test_type == GAP_TEST:
            # Errors
            test_markdown.append(
                self._df_to_markdown(
                    pandas.DataFrame(
                        _format_errors(rd, include_node_type=True)
                    ).sort_values(by="Errors")
                )
            )
            # File locations
            test_markdown.append(
                self._df_to_markdown(
                    pandas.DataFrame(
                        _format_locations(rd, include_node_type=True)
                    ).sort_values(by="Locations")
                )
            )

        elif test_type == REL_TEST:
            # -- Errors --
            err_rollup = False
            from_type = rd["inputs"]["from"]
            to_type = rd["inputs"]["to"]

            # Rollup error dicts into 1 line when all from_nodes are
            # linked to 0 to_nodes (e.g.
            # All PARTICIPANT.ID are linked to 0 FAMILY.ID)
            if (len(rd["errors"]) == self.counts[from_type]) and all(
                [not e["to"] for e in rd["errors"]]
            ):
                err_rollup = True
                error_rows = [
                    {
                        "Errors": f"All {self.counts[from_type]} "
                        f"{_style(from_type)} in the dataset "
                        f"are linked to 0 {_style(to_type)}"
                    }
                ]
            # Transform each error dict into markdown text
            else:
                error_rows = _format_errors(rd)

            test_markdown.append(
                self._df_to_markdown(
                    pandas.DataFrame(error_rows).sort_values(by="Errors")
                )
            )

            # -- File locations --
            # Table of file paths - show this when we've rolled up errors
            if err_rollup:
                location_rows = [
                    {"Locations": row["Locations"]}
                    for row in _format_locations(rd)
                ]
            # Table of file paths and error nodes
            else:
                location_rows = _format_locations(rd)

            test_markdown.append(
                self._df_to_markdown(
                    pandas.DataFrame(location_rows).sort_values(by="Locations")
                )
            )

        return "\n".join(test_markdown)

    def _df_to_markdown(self, df):
        """
        Transform pandas.DataFrame into markdown text. If number of DataFrame
        rows is >= ROW_LIMIT, encapsulate the table in a collapsible markdown
        section
        :param df: the df to transform
        :type df: pandas.DataFrame,
        :param table_name: optional name of table to include in collapsible
        summary
        :type table_name: str,
        :returns: markdown formatted string
        """
        output = []

        # DataFrame to markdown table
        md_table = df.to_markdown(index=False)

        # Encapsulate table in collapsible section if we have too many rows
        if len(df) >= ROW_LIMIT:
            output.append('<details markdown="1">')
            output.append("<summary><b>Click to expand table</b></summary>")
            output.append("")
            output.append(f"{md_table}")
            output.append("")
            output.append("</details>")
            output.append("")
        else:
            output.append("")
            output.append(md_table)

        return "\n".join(output)

    def _counts_md(self, results):
        """
        Create markdown formatted table of entity type counts

        :param results: See sample_validation_results.py for `results` format
        :type results: list of dicts

        :returns: markdown formatted str
        """
        return self._df_to_markdown(
            pandas.DataFrame(
                [
                    {"Entity": typ, "Count": count}
                    for typ, count in results["counts"].items()
                ]
            )
            .replace(r"\|", REPLACE_PIPE, regex=True)
            .sort_values(by="Count", ascending=False)
            .applymap(md_clean)
        )

    def _files_md(self, results):
        """
        Create markdown formatted table of files evaluated

        :param results: See sample_validation_results.py for `results` format
        :type results: list of dicts

        :returns: markdown formatted str
        """
        output = []

        # Organize files by directory
        files = defaultdict(set)
        for file in self.files_validated:
            d, fn = os.path.split(file)
            files[d].add(fn)

        for d, file_names in files.items():
            output.append("<br>")
            output.append("")
            output.append(f"**{md_clean(d)}**")
            output.append(
                self._df_to_markdown(
                    pandas.DataFrame({"Files": md_clean(sorted(file_names))})
                )
            )
            output.append("")

        return "\n".join(output)
