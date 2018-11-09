import json
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

    def __init__(self, study_config_dir, extract_config_paths):
        super().__init__(study_config_dir)
        if isinstance(extract_config_paths, list):
            self.extract_configs = [ExtractConfig(config_filepath)
                                    for config_filepath
                                    in extract_config_paths]

    def _serialize_output(self, output):
        """
        Implements IngestStage._serialize_output
        """
        class IndexlessJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if hasattr(obj, 'to_dict'):
                    no_index = obj.to_dict(orient='split')
                    del no_index['index']
                    return no_index
                return json.JSONEncoder.default(self, obj)

        return json.dumps(output, cls=IndexlessJSONEncoder, indent=2)

    def _deserialize_output(self, serialized_output):
        """
        Implements IngestStage._deserialize_output
        """
        data = json.loads(serialized_output)
        for k, v in data.items():
            v[1] = pandas.DataFrame.from_records(
                v[1]['data'],
                columns=v[1]['columns']
            )
        return data

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
        self.logger.debug(msg)

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
