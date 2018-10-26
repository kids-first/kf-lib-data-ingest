import os
from collections import defaultdict

import pandas

from common.datafile_readers import read_excel_file
from common.file_retriever import PROTOCOL_SEP, FileRetriever, split_protocol
from common.misc import intsafe_str
from common.stage import IngestStage
from common.type_safety import assert_safe_type, function
from etl.configuration.base_config import ConfigValidationError
from etl.configuration.extract_config import ExtractConfig


def is_multiple(a, b):
    if (a == 0) or (b == 0) or ((max(a, b) % min(a, b)) == 0):
        return True
    return False


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
        output = {}
        for extract_config in self.extract_configs:
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

            load_args = extract_config.loading_params.copy()
            load_func = None
            if 'load_func' in load_args:
                load_func = load_args['load_func']
                del load_args['load_func']

            assert_safe_type(load_func, None, function)

            # read contents from file
            df_in = self._source_file_to_df(
                data_path, load_func=load_func, **load_args
            ).applymap(intsafe_str)

            # extraction
            df_out = self._chain_operations(
                df_in, extract_config.operations
            )

            output[extract_config.config_filepath] = df_out

        # return dictionary of all dataframes keyed by extract config paths
        return output

    def _source_file_to_df(self, file_path, load_func=None, **load_args):
        """
        Load the file using either load_func if given or according to the file
        extension otherwise. Any load_args get forwarded to the load function.
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
        out_cols = defaultdict(pandas.Series)
        max_col_length = 0

        for op in operations:
            if function(op):
                res = op(df_in)
            else:  # list
                res = self._chain_operations(df_in, op)

            for col_name, col_series in res.iteritems():
                assert is_multiple(max_col_length, len(col_series.index))
                out_cols[col_name] = out_cols[col_name].append(
                    col_series, ignore_index=True
                )
                max_col_length = max(
                    max_col_length, len(out_cols[col_name].index)
                )

        df_out = pandas.DataFrame()
        for col_name, col_series in out_cols.items():
            col_len = len(col_series.index)
            if col_len:
                length_multiplier = max_col_length / col_len
                assert length_multiplier == round(length_multiplier)
                try:
                    df_out[col_name] = pandas.Series(
                        list(col_series) * int(length_multiplier)
                    )
                except Exception as e:
                    print(e)
                    breakpoint()

        return df_out
