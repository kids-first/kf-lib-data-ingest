import os
from collections import defaultdict
from functools import reduce
from math import gcd
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.datafile_readers import read_excel_file
from kf_lib_data_ingest.common.file_retriever import (
    PROTOCOL_SEP,
    FileRetriever,
    split_protocol
)
from kf_lib_data_ingest.common.misc import (
    intsafe_str,
    write_json,
    read_json
)
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import function
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError
)
from kf_lib_data_ingest.etl.configuration.extract_config import ExtractConfig

is_function = function  # simple alias for clarity


def lcm(number_list):
    """
    Returns the least common multiple from a list of numbers.
    """
    return reduce(
        lambda x, y: x * y // gcd(x, y), number_list
    )


class ExtractStage(IngestStage):
    def __init__(
        self, stage_cache_dir, extract_config_paths
    ):
        super().__init__(stage_cache_dir)
        if isinstance(extract_config_paths, list):
            self.extract_configs = [ExtractConfig(config_filepath)
                                    for config_filepath
                                    in extract_config_paths]

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
            df.to_csv(filepath, sep='\t', index=False)

        write_json(metadata, meta_fp)

        self.logger.info(f'Writing {self.__class__.__name__} output:\n'
                         f'{pformat(list(output.keys()))}')

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
                pandas.read_csv(filepath, delimiter='\t')
            )

        self.logger.info(f'Reading {self.__class__.__name__} output:\n'
                         f'{pformat(list(metadata.keys()))}')
        return output

    def _validate_run_parameters(self):
        # Extract stage does not expect any args
        pass

    def _log_operation(self, op):
        """Log execution of an extract operation.

        :param op: an extract operation
        :type op: function
        """
        msg = f'Applying {op.__qualname__}'
        if op.__closure__:
            msg += ' with ' + str({
                k: v.cell_contents for k, v
                in zip(op.__code__.co_freevars, op.__closure__)
            })
        self.logger.info(msg)

    def _run(self):
        """
        :returns: A dictionary where a key is the URL to the extract_config
            that produced the dataframe and a value is a tuple containing:
            (<URL to source data fle>, <extracted DataFrame>)
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
            df_in = self._source_file_to_df(
                data_path,
                after_load=extract_config.after_load,
                load_func=extract_config.loading_func,
                **(extract_config.loading_params)
            ).applymap(intsafe_str)

            # extraction
            df_out = self._chain_operations(
                df_in, extract_config.operations
            )

            output[extract_config.config_filepath] = (data_path, df_out)

        # return dictionary of all dataframes keyed by extract config paths
        return output

    def _source_file_to_df(
        self, file_path, after_load=None, load_func=None, **load_args
    ):
        """
        Load the file using either load_func if given or according to the file
        extension otherwise. Any load_args get forwarded to the load function.

        :param file_path: <protocol>://<path> for a source data file
        :param load_func: A function used for custom loading
        :kwargs load_args: Options passed to the loading functions

        :returns: A pandas dataframe containing the requested data
        """
        f = FileRetriever().get(file_path)
        if load_func:
            df = load_func(f, **load_args)
        else:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                df = read_excel_file(f, **load_args)
            elif file_path.endswith('.tsv'):
                df = pandas.read_table(f, sep='\t', **load_args)
            elif file_path.endswith('.csv'):
                df = pandas.read_table(f, sep=',', **load_args)
            else:
                raise ConfigValidationError(
                    "Could not determine appropriate loader for data file",
                    file_path
                )
        if after_load:
            return after_load(df)
        return df

    def _chain_operations(self, df_in, operations):
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
        for op in operations:
            # apply operation(s), get result
            if is_function(op):
                self._log_operation(op)
                res = op(df_in)
            else:  # list
                res = self._chain_operations(df_in, op)

            # result length must be a whole multiple of the original length,
            # otherwise we've lost rows
            assert res.index.size % original_length == 0

            for col_name, col_series in res.iteritems():
                out_cols[col_name] = out_cols[col_name].append(
                    col_series, ignore_index=True
                )

        # the output dataframe length will be the least common multiple of the
        # extracted column lengths
        length_lcm = lcm(list(map(len, out_cols.values())))

        # Given a set of different length columns, we need to make a resulting
        # dataframe whose length is the least common multiple of their lengths
        # by repeating each column the right number of times.
        #
        # A B C                     A B C
        # 1 1 1     will become     1 1 1
        #   2 2                     1 2 2
        #     3                     1 1 3
        #                           1 2 1
        #                           1 1 2
        #                           1 2 3
        #
        df_out = pandas.DataFrame()
        for col_name, col_series in out_cols.items():
            if not col_series.empty:
                length_multiplier = length_lcm / col_series.size
                assert length_multiplier == round(length_multiplier)
                # repeat the series length_multiplier times
                df_out[col_name] = pandas.Series(
                    list(col_series) * round(length_multiplier)
                )

        return df_out
