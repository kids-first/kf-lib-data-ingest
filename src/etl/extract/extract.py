import os
from collections import defaultdict
from functools import reduce
from math import gcd

import pandas

from common.datafile_readers import read_excel_file
from common.file_retriever import PROTOCOL_SEP, FileRetriever, split_protocol
from common.misc import intsafe_str
from common.stage import IngestStage
from common.type_safety import assert_safe_type, function
from etl.configuration.base_config import ConfigValidationError
from etl.configuration.extract_config import ExtractConfig

is_function = function  # simple alias for clarity


def lcm(number_list):
    """
    Returns the least common multiple from a list of numbers.
    """
    return reduce(
        lambda x, y: x*y//gcd(x, y), number_list
    )


class ExtractStage(IngestStage):

    def __init__(self, extract_config_paths):
        super().__init__()
        if isinstance(extract_config_paths, list):
            self.extract_configs = [ExtractConfig(config_filepath)
                                    for config_filepath
                                    in extract_config_paths]

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _validate_run_parameters(self):
        # Extract stage does not expect any args so we can pass validation
        pass

    def _run(self):
        """
        :returns: A dictionary of all extracted dataframes keyed by extract
            config paths
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

    def _source_file_to_df(self, file_path, load_func=None, **load_args):
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
            return load_func(file_path, **load_args)
        else:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                return read_excel_file(file_path, **load_args)
            elif file_path.endswith('.tsv'):
                return pandas.read_table(file_path, sep='\t', **load_args)
            elif file_path.endswith('.csv'):
                return pandas.read_table(file_path, sep=',', **load_args)
            else:
                raise ConfigValidationError(
                    "Could not determine appropriate loader for data file",
                    file_path
                )

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
