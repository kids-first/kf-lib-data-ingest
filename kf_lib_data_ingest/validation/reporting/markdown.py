"""
Markdown validation report builder
Produces a human friendly markdown based validation report

Extends kf_lib_data_ingest.validation.reporting.base.AbstractReportBuilder
"""
import os
from collections import defaultdict
import pandas

from kf_lib_data_ingest.validation.reporting.base import (
    AbstractReportBuilder,
    RESULT_TO_EMOJI,
    PASSED,
    FAILED,
    NA,
)

DEFAULT_REPORT_TITLE = "📓 Data Validation Report"
RESULTS_FILENAME = "validation_results.md"
REPLACE_PIPE = "."
REL_TEST = "relationship"
ATTR_TEST = "attribute"
GAP_TEST = "gaps"
ERROR_FRAC_THRESHOLD = 0.50
ERROR_COUNT_THRESHOLD = 20


class MarkdownReportBuilder(AbstractReportBuilder):
    def __init__(self, output_dir=None, setup_logger=False):
        """
        Constructor

        See AbstractReportBuilder.__init__
        """
        super().__init__(output_dir=output_dir, setup_logger=setup_logger)

    def _build(self, results, title=DEFAULT_REPORT_TITLE):
        """
        Build markdown content for validation report

        :param results: See AbstractReportBuilder._build
        :param type: See AbstractReportBuilder._build
        :param title: Report title
        :type title: str
        """
        self.counts = results["counts"]
        self.files_validated = results["files_validated"]
        self.results = results["validation"]

        output = []
        output.append(f"# {title}")
        output.append("## 📂 Files Validated")
        output.append(self._files_md(results))
        output.append("\n##  #️⃣  Counts")
        output.append(self._counts_md(results))
        output.append("\n## 🚦 Relationship Tests")
        output.append(self._tests_section_md(results, REL_TEST))
        output.append("\n## 🚦 Gap Tests")
        output.append(self._tests_section_md(results, GAP_TEST))
        output.append("\n## 🚦 Attribute Value Tests")
        output.append(self._tests_section_md(results, ATTR_TEST))
        output.append("\n## 🚦 Count Tests")
        output.append(self._tests_section_md(results, "count"))
        output = "\n".join(output)

        return output

    def _write_report(self, content):
        """
        Write markdown validation report file to disk
        """
        output_path = os.path.join(self.output_dir, RESULTS_FILENAME)
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
        header = (
            f"{RESULT_TO_EMOJI.get(self._result_code(result_dict))} "
            f'{result_dict["description"]}'
        ).replace("|", REPLACE_PIPE)

        # Make test header stand out for tests that ran
        if result_dict["is_applicable"]:
            header = f"#### {header}"

        return header

    def _tests_section_md(self, results, test_type):
        """
        Create markdown section containing test results for a given
        test type: [count | attribute | relationship | gap]

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
        summary = " ".join(
            [
                f"`{code.title()}: {len(test_markdowns.get(code, []))}`"
                for code in test_order_by_outcome
            ]
        )
        output.append(f"### {summary}")

        # Add test markdowns
        for result_code in test_order_by_outcome:
            tm = test_markdowns.get(result_code, [])
            output.extend(tm)

        return "\n".join(output)

    def _result_to_md(self, rd, test_type):
        """
        Helper method for _tests_section_md. Convert single test result dict
        into markdown formatted str
        """
        def _style(val):
            """
            Style string val to make it stand out in a markdown doc
            Replace | char with .
            """
            return f'`{str(val).replace("|", REPLACE_PIPE)}`'

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

                errors.append({"Errors": f"{prefix} {suffix}"})
            return errors

        def _format_locations(result_dict, include_node_type=False):
            """
            Transform location dicts into formatted location strings
            Return list of location row dicts for DataFrame
            """
            locs = defaultdict(set)
            # -- File locations --
            # Organize error values by file they were found in
            for e in result_dict["errors"]:
                for (typ, val), files in e["locations"].items():
                    for f in files:
                        if include_node_type:
                            val = tuple_to_str((typ, val))
                        else:
                            val = _style(val)
                        locs[f].add(val)

            # Rollup locations - if number of error values in a file ==
            # number of total errors for the test, then don't list out every
            # error value in that file
            loc_rows = []
            for loc, vals in locs.items():
                if len(vals) == len(rd["errors"]):
                    val_str = "This file contains all errors in the **Errors** table above."
                else:
                    val_str = ",".join(sorted(vals))
                loc_rows.append(
                    {"Location": os.path.basename(loc), "Values": val_str}
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
        if test_type == "count":
            test_markdown.append(
                f'Found: {_style(rd["errors"]["found"])} '
                f'but expected: {_style(rd["errors"]["expected"])}'
            )

        elif test_type == ATTR_TEST:
            test_markdown.append(
                pandas.DataFrame(
                    [
                        {
                            "Location": os.path.basename(file_path),
                            "Bad Values": ",".join(
                                sorted([_style(v) for v in bad_vals])
                            ),
                        }
                        for file_path, bad_vals in rd["errors"].items()
                    ]
                ).to_markdown(index=False)
            )
        elif test_type == GAP_TEST:
            # Errors
            test_markdown.append(
                pandas.DataFrame(
                    _format_errors(rd, include_node_type=True)
                ).to_markdown(index=False)
            )
            test_markdown.append("")
            # File locations
            test_markdown.append(
                pandas.DataFrame(_format_locations(rd, include_node_type=True))
                .sort_values(by="Location")
                .to_markdown(index=False)
            )

        elif test_type == REL_TEST:
            # -- Errors --
            err_rollup = False
            from_type = rd["inputs"]["from"]
            to_type = rd["inputs"]["to"]
            error_count = len(rd["errors"])
            error_fraction = error_count / self.counts[from_type]

            # Rollup errors into 1 line when a large enough % of from_nodes are
            # linked to 0 to_nodes (e.g. All PARTICIPANT.ID are linked to [])
            if (
                (error_count >= ERROR_COUNT_THRESHOLD) and
                (error_fraction >= ERROR_FRAC_THRESHOLD) and
                all(
                    [not e["to"] for e in rd["errors"]]
                )
            ):
                err_rollup = True
                error_rows = [
                    {
                        "Errors": f"At least {error_fraction * 100} % of "
                        f"{self.counts[from_type]} "
                        f"{_style(from_type)} in the dataset "
                        f"are linked to 0 {_style(to_type)}"
                    }
                ]
            # Create formatted error strings
            else:
                error_rows = _format_errors(rd)

            test_markdown.append(
                pandas.DataFrame(error_rows).to_markdown(index=False)
            )

            # -- File locations --
            # Only show file locations when we have not rolled up the
            # errors into a 1 line error statement
            if not err_rollup:
                test_markdown.append("")
                test_markdown.append(
                    pandas.DataFrame(_format_locations(rd))
                    .sort_values(by="Location")
                    .to_markdown(index=False)
                )

        return "\n".join(test_markdown)

    def _counts_md(self, results):
        """
        Create markdown formatted table of entity type counts

        :param results: See sample_validation_results.py for `results` format
        :type results: list of dicts

        :returns: markdown formatted str
        """
        return (
            pandas.DataFrame(
                [
                    {"Entity": typ, "Count": count}
                    for typ, count in results["counts"].items()
                ]
            )
            .replace(r"\|", REPLACE_PIPE, regex=True)
            .sort_values("Count", ascending=False)
            .to_markdown(index=False)
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
            output.append(f"**{d}**\n")
            output.append(
                pandas.DataFrame([{"Files": fn} for fn in file_names])
                .sort_values(by="Files")
                .to_markdown(index=False)
            )
            output.append("")

        return "\n".join(output)
