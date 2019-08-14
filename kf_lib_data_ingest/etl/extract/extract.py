import os
import sys
from collections import defaultdict
from functools import reduce
from math import gcd
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.concept_schema import concept_set
from kf_lib_data_ingest.common.datafile_readers import (
    read_excel_file,
    read_json_file,
    read_table_file
)
from kf_lib_data_ingest.common.file_retriever import (
    PROTOCOL_SEP,
    FileRetriever,
    split_protocol
)
from kf_lib_data_ingest.common.misc import (
    clean_up_df,
    clean_walk,
    read_json,
    write_json
)
from kf_lib_data_ingest.common.pandas_utils import split_df_rows_on_splits
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
    is_function
)
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.configuration.extract_config import ExtractConfig
from kf_lib_data_ingest.etl.extract import operations as extract_operations


def lcm(number_list):
    """
    Returns the least common multiple from a list of numbers.
    """
    return reduce(
        lambda x, y: x * y // gcd(x, y), number_list
    )


def ordinal(n):
    """
    Convert a positive integer into its ordinal representation
    """
    suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    return str(n) + suffix


class ExtractStage(IngestStage):
    def __init__(
        self, stage_cache_dir, extract_config_dir, auth_configs=None
    ):
        super().__init__(stage_cache_dir)

        assert_safe_type(extract_config_dir, str)

        # must set FileRetriever.static_auth_configs before extract configs are
        # loaded
        FileRetriever.static_auth_configs = auth_configs
        self.FR = FileRetriever()

        self.extract_config_dir = extract_config_dir
        self.extract_configs = [
            ExtractConfig(filepath)
            for filepath in clean_walk(self.extract_config_dir)
            if filepath.endswith('.py')
        ]
        for ec in self.extract_configs:
            ec.config_file_relpath = os.path.relpath(
                ec.config_filepath,
                start=self.extract_config_dir
            )

    def _output_path(self):
        """
        Construct the filepath of the output.
        Something like:
            <study config dir path>/output_cache/<ingest stage class name>.xxx

        :return: file location to put/get serialized output for this stage
        :rtype: string
        """
        return os.path.join(
            self.stage_cache_dir, type(self).__name__ + '_cache.txt'
        )

    def _write_output(self, output):
        """
        Implements IngestStage._write_output

        Write dataframes to tsv files in the stage's output dir.
        Write a JSON file that stores metadata needed to reconstruct the output
        dict. This is the extract_config_url and source_url for each file.

        The metadata.json file looks like this:

        {
            <path to output file>: {
                'extract_config_url': <URL to the files's extract config>
                'source_data_url': <URL to the original data file>
            },
            ...
        }

        :param output: the return from ExtractStage._run
        :type output: dict
        """
        meta_fp = os.path.join(self.stage_cache_dir, 'metadata.json')
        metadata = {}
        for extract_config_url, (source_url, df) in output.items():
            filename = os.path.basename(extract_config_url).split('.')[0]
            filepath = os.path.join(self.stage_cache_dir, filename + '.tsv')
            metadata[filepath] = {
                'extract_config_url': extract_config_url,
                'source_data_url': source_url
            }
            df.to_csv(filepath, sep='\t', index=True)

        write_json(metadata, meta_fp)

    def _read_output(self):
        """
        Implements IngestStage._write_output

        Read in the output files created by _write_output and reconstruct the
        original output of ExtractStage._run.

        :returns the original output of ExtractStage._run.
        """
        output = {}

        meta_fp = os.path.join(self.stage_cache_dir, 'metadata.json')
        metadata = read_json(meta_fp)

        for filepath, info in metadata.items():
            output[info['extract_config_url']] = (
                info['source_data_url'],
                read_table_file(filepath, delimiter='\t', index_col=0)
            )

        self.logger.info(f'Reading {type(self).__name__} output:\n'
                         f'{pformat(list(metadata.keys()))}')
        return output

    def _validate_run_parameters(self):
        # Extract stage does not expect any args
        pass

    def _log_operation(self, op, nth):
        """Log execution of an extract operation.

        :param op: an extract operation
        :type op: function
        :param nth: which operation number
        :type nth: int
        """
        opname = op.__qualname__
        if op.__module__ == extract_operations.__name__:
            opname = opname.split('.')[0]
        msg = f'Applying {ordinal(nth)} operation: {opname}'
        if op.__closure__:
            msg += ' with ' + str({
                k: v.cell_contents for k, v
                in zip(op.__code__.co_freevars, op.__closure__)
            })
        self.logger.info(msg)

    def _obvert_visibility(self, df):
        """
        If something is visible, then also record that it's not hidden, and
        vice versa.
        """

        def flip_col(in_col):
            out_col = in_col.copy()
            out_col[out_col.notnull()] = (
                ~in_col[in_col.notnull()].astype(bool)
            ).astype(object)
            return out_col

        for concept in concept_set:
            if concept.HIDDEN in df and concept.VISIBLE not in df:
                df[concept.VISIBLE] = flip_col(df[concept.HIDDEN])
            elif concept.VISIBLE in df and concept.HIDDEN not in df:
                df[concept.HIDDEN] = flip_col(df[concept.VISIBLE])
            elif concept.VISIBLE in df and concept.HIDDEN in df:
                assert df[concept.HIDDEN].equals(flip_col(df[concept.VISIBLE]))

    def _source_file_to_df(
        self, file_path, do_after_load=None, load_func=None, **load_args
    ):
        """
        Load the file using either load_func if given or according to the file
        extension otherwise. Any load_args get forwarded to the load function.

        :param file_path: <protocol>://<path> for a source data file
        :param load_func: A function used for custom loading
        :kwargs load_args: Options passed to the loading functions

        :returns: A pandas dataframe containing the requested data
        """
        self.logger.debug("Retrieving source file %s", file_path)
        f = self.FR.get(file_path)

        err = None
        if load_func:
            self.logger.info("Using custom load_func function.")
        else:
            if f.original_name.endswith(('.xlsx', '.xls')):
                load_func = read_excel_file
            elif f.original_name.endswith(('.tsv', '.csv')):
                load_func = read_table_file
            elif f.original_name.endswith('.json'):
                load_func = read_json_file

        if load_func:
            try:
                df = load_func(f.name, **load_args)
                if not isinstance(df, pandas.DataFrame):
                    err = (
                        "Custom load_func must return a pandas.DataFrame"
                    )
            except Exception as e:
                err = f"In load_func {load_func.__name__} : {str(e)}"
        else:
            err = (
                f"Could not determine appropriate loader for '{file_path}'."
                "\nYou may need to define a custom load_func function."
            )

        if err:
            raise ConfigValidationError(err)

        df = clean_up_df(df)

        if do_after_load:
            self.logger.info("Calling custom do_after_load function.")
            df = do_after_load(df)

        return df

    def _chain_operations(self, df_in, operations, _nth=1, _is_nested=False):
        """
        Performs the operations sequence for extracting columns of data from
        the source data files.

        :param df_in: A pandas dataframe containing all of the file data
        :param operations: See the extract_config_format spec for the
            the operations sequence
        :returns: A pandas dataframe containing extracted mapped data
        """
        out_cols = defaultdict(pandas.Series)
        original_length = df_in.index.size

        # collect columns of extracted data
        for i, op in enumerate(operations):
            # apply operation(s), get result
            if is_function(op):
                self._log_operation(op, i+_nth)
                res = op(df_in)

                # result length must be a whole multiple of the original
                # length, otherwise we've lost rows
                if res.index.size % original_length != 0:
                    raise ConfigValidationError(
                        "Operation result length is not a multiple of the "
                        "source data length."
                    )

                if (res.index.size / original_length > 1) and not _is_nested:
                    raise ConfigValidationError(
                        "Operations returning results longer than the source "
                        "data length are required to be nested in "
                        "context-appropriate sublists as a safeguard against "
                        "accidental misuse. To learn about the importance of "
                        "nested operation sublists, read "
                        "https://kids-first.github.io/kf-lib-data-ingest/design/extract_mapping.html#nested-operations-sublists"  # noqa E501
                    )
            else:  # list
                self.logger.info("Diving into nested operation sublist.")
                res = self._chain_operations(df_in, op, i+_nth, True)

            for col_name, col_series in res.iteritems():
                out_cols[col_name] = out_cols[col_name].append(
                    col_series, ignore_index=False
                )

        self.logger.info("Done with the operations list.")

        # the output dataframe length will be the least common multiple of the
        # extracted column lengths
        length_lcm = lcm(list(map(len, out_cols.values())))

        self.logger.debug("Extracted column lengths are:")
        for col_name, col_series in out_cols.items():
            self.logger.debug("- %s: %d", col_name, col_series.size)

        self.logger.info(
            "Equalizing column lengths to the LCM: %d", length_lcm
        )

        # Given a set of different length columns, we need to make a resulting
        # dataframe whose length is the least common multiple of their lengths
        # by repeating each column the right number of times. This is
        # predicated on the assumption that no rows were added or removed.
        #
        # A data file that looks like...
        #
        #  index   A   B   C
        #  0       1   3   5
        #  1       2   4   6
        #
        # ... (with B and C each being melted) temporarily extracts to ...
        #
        #  A_index A | DESCRIPTION_index DESCRIPTION | VALUE_index VALUE
        #  0       1 | 0                 B           | 0           3
        #  1       2 | 1                 B           | 1           4
        #            | 0                 C           | 0           5
        #            | 1                 C           | 1           6
        #
        # ... and will then repeat column A here to fill the DataFrame ...
        #
        #  index   A   DESCRIPTION   VALUE
        #  0       1   B             3
        #  1       2   B             4
        #  0       1   C             5
        #  1       2   C             6

        df_out = pandas.DataFrame(dtype=object)
        index = None
        for col_name, col_series in out_cols.items():
            if not col_series.empty:
                length_multiplier = length_lcm / col_series.size
                assert length_multiplier == round(length_multiplier)
                # repeat the series length_multiplier times
                df_out[col_name] = pandas.Series(
                    list(col_series) * round(length_multiplier), dtype=object
                )
                col_index = list(col_series.index) * round(length_multiplier)
                if index:
                    if index != col_index:
                        raise Exception(
                            "Inconsistent column indices.", index, col_index
                        )
                else:
                    index = col_index
        df_out.index = index
        return df_out

    def _run(self):
        """
        :returns: A dictionary where a key is the URL to the extract_config
            that produced the dataframe and a value is a tuple containing:
            (<URL to source data file>, <extracted DataFrame>)
        """
        output = {}
        for extract_config in self.extract_configs:
            self.logger.info(
                "Extract config: %s", extract_config.config_filepath
            )
            protocol, path = split_protocol(extract_config.source_data_url)
            if protocol == 'file':
                if path.startswith('.'):
                    # relative paths from the extract config location
                    path = os.path.normpath(
                        os.path.join(
                            os.path.dirname(extract_config.config_filepath),
                            path
                        )
                    )
                else:
                    path = os.path.expanduser(path)

            data_path = protocol + PROTOCOL_SEP + path

            # read contents from file
            try:
                df_in = self._source_file_to_df(
                    data_path,
                    do_after_load=extract_config.do_after_load,
                    load_func=extract_config.load_func,
                    **(extract_config.loading_params)
                )
            except ConfigValidationError as e:
                raise type(e)(
                    f'In extract config {extract_config.config_filepath}'
                    f' : {str(e)}'
                )

            self.logger.debug(
                f'Loaded DataFrame with dimensions {df_in.shape}'
            )
            self.logger.debug(f'Column headers are: {df_in.columns}')

            if not extract_config.operations:
                self.logger.info("The operation list is empty. Nothing to do.")
                return output

            # extraction
            df_out = self._chain_operations(
                df_in, extract_config.operations
            )

            # split value lists into separate rows
            df_out = split_df_rows_on_splits(
                df_out.reset_index()
            ).set_index('index')
            del df_out.index.name

            # VISIBLE = not HIDDEN
            self._obvert_visibility(df_out)

            # standardize values again after operations
            df_out = clean_up_df(df_out)

            output[extract_config.config_file_relpath] = (data_path, df_out)

        # return dictionary of all dataframes keyed by extract config paths
        return output

    def _postrun_concept_discovery(self, run_output):
        """
        See the docstring for IngestStage._postrun_concept_discovery
        """
        sources = defaultdict(
            lambda: defaultdict(set)
        )
        links = defaultdict(
            lambda: defaultdict(set)
        )

        for config_path, (data_file, df) in run_output.items():
            cols = df.columns
            self.logger.debug(f'Recording {config_path} sources')
            for key in cols:
                sk = sources[key]
                for val in df[key]:
                    # sources entry
                    sk[val].add(data_file)

            self.logger.debug(f'Recording {config_path} links')
            for _, row in df.iterrows():
                for keyA, vA in row.items():
                    lk_prefix = keyA + '::'
                    if vA:
                        for keyB, vB in row.items():
                            if vB and (keyB != keyA):
                                # links entry
                                links[lk_prefix + keyB][vA].add(vB)

        return {'sources': sources, 'links': links, 'values': None}
