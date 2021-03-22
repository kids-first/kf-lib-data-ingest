import os
from pprint import pformat

import pandas

from kf_lib_data_ingest.common.file_retriever import (
    PROTOCOL_SEP,
    FileRetriever,
    split_protocol,
)
from kf_lib_data_ingest.common.io import read_df, read_json, write_json
from kf_lib_data_ingest.common.misc import clean_up_df, clean_walk
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import assert_safe_type
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.extract_config import ExtractConfig
from kf_lib_data_ingest.etl.extract.utils import Extractor


class ExtractStage(IngestStage):
    def __init__(self, stage_cache_dir, extract_config_dir, auth_configs=None):
        super().__init__(stage_cache_dir)

        assert_safe_type(extract_config_dir, str)

        # must set FileRetriever.static_auth_configs before extract configs are
        # read
        FileRetriever.static_auth_configs = auth_configs
        self.FR = FileRetriever()

        self.extract_config_dir = extract_config_dir
        self.extract_configs = [
            ExtractConfig(filepath)
            for filepath in clean_walk(self.extract_config_dir)
            if filepath.endswith(".py")
        ]
        for ec in self.extract_configs:
            ec.config_file_relpath = os.path.relpath(
                ec.config_filepath, start=self.extract_config_dir
            )
        self.extractor = Extractor()

    def _output_path(self):
        """
        Construct the filepath of the output.
        Something like:
            <project config dir path>/output_cache/<ingest stage class name>.xxx

        :return: file location to put/get serialized output for this stage
        :rtype: string
        """
        return os.path.join(
            self.stage_cache_dir, type(self).__name__ + "_cache.txt"
        )

    def _write_output(self, output):
        """
        Implements IngestStage._write_output

        Write dataframes to tsv files in the stage's output dir.
        Write a JSON file that stores metadata needed to reconstruct the output
        dict. This is the extract_config_url and source_url for each file.

        The metadata.json file looks like this:

        {
            <path to output file>: <URL to the files's extract config>,
            ...
        }

        :param output: the return from ExtractStage._run
        :type output: dict
        """
        meta_fp = os.path.join(self.stage_cache_dir, "metadata.json")
        metadata = {}
        for extract_config_url, df in output.items():
            filename = os.path.basename(extract_config_url).split(".")[0]
            filepath = os.path.join(self.stage_cache_dir, filename + ".tsv")
            metadata[extract_config_url] = filepath
            df.to_csv(filepath, sep="\t", index=True)

        write_json(metadata, meta_fp)

    def _read_output(self):
        """
        Implements IngestStage._write_output

        Read in the output files created by _write_output and reconstruct the
        original output of ExtractStage._run.

        :return: the original output of ExtractStage._run.
        """
        output = {}

        meta_fp = os.path.join(self.stage_cache_dir, "metadata.json")
        metadata = read_json(meta_fp)

        for extract_config_url, filepath in metadata.items():
            output[extract_config_url] = clean_up_df(
                read_df(filepath, delimiter="\t", index_col=0)
            )

        self.logger.info(
            f"Reading {type(self).__name__} output:\n"
            f"{pformat(list(metadata.keys()))}"
        )
        return output

    def _validate_run_parameters(self, _ignore=None):
        # Extract stage does not expect any args
        pass

    def _source_file_to_df(
        self, file_path, do_after_read=None, read_func=None, **read_args
    ):
        """
        Read the file using either read_func if given or according to the file
        extension otherwise. Any read_args get forwarded to the read function.

        :param file_path: <protocol>://<path> for a source data file
        :param read_func: A function used for custom reading
        :kwargs read_args: Options passed to the reading functions

        :return: A pandas dataframe containing the requested data
        """
        self.logger.debug("Retrieving source file %s", file_path)
        f = self.FR.get(file_path)

        err = None
        try:
            if read_func:
                self.logger.info("Using custom read function.")
                df = read_func(f.name, **read_args)
            else:
                df = read_df(f.name, f.original_name, **read_args)
            if not isinstance(df, pandas.DataFrame):
                err = "Read function must return a pandas.DataFrame"
        except Exception as e:
            err = (
                f"Error in {read_func.__name__} : {str(e)}"
                if read_func
                else str(e)
            )

        if err:
            raise ConfigValidationError(
                f"Reading file '{f.original_name}' from '{file_path}'."
                f" {err} ('{f.name}')"
            )

        df = clean_up_df(df)

        if do_after_read:
            self.logger.info("Calling custom do_after_read function.")
            df = do_after_read(df)

        return df

    def _run(self, _ignore=None):
        """
        :return: A dictionary where a key is the URL to the extract_config
            that produced the dataframe and a value is a tuple containing:
            (<URL to source data file>, <extracted DataFrame>)
        :rtype: dict
        """
        output = {}
        self.messages = []
        for extract_config in self.extract_configs:
            self.logger.info(
                "Extract config: %s", extract_config.config_filepath
            )
            protocol, path = split_protocol(extract_config.source_data_url)
            if protocol == "file":
                if path.startswith("."):
                    # relative paths from the extract config location
                    path = os.path.normpath(
                        os.path.join(
                            os.path.dirname(extract_config.config_filepath),
                            path,
                        )
                    )
                else:
                    path = os.path.expanduser(path)

            data_path = protocol + PROTOCOL_SEP + path

            # read contents from file
            try:
                df_in = self._source_file_to_df(
                    data_path,
                    do_after_read=extract_config.do_after_read,
                    read_func=extract_config.source_data_read_func,
                    **(extract_config.source_data_read_params or {}),
                )
            except ConfigValidationError as e:
                raise type(e)(
                    f"In extract config {extract_config.config_filepath}"
                    f" : {str(e)}"
                )

            if len(df_in.index) == 0:
                fnames = []
                if extract_config.source_data_read_func:
                    fnames.append("source_data_read_func")
                if extract_config.do_after_read:
                    fnames.append("do_after_read")
                msg = "Source DataFrame is empty."
                if fnames:
                    suffix = "s" if (len(fnames) > 1) else ""
                    fnames = " and ".join(fnames)
                    msg = f"{msg} Check your {fnames} function{suffix}."
                raise ConfigValidationError(msg)

            df_out = self.extractor.extract(
                df_in, extract_config, apply_after_read_func=False
            )
            self.messages.extend(self.extractor.messages)
            output[extract_config.config_file_relpath] = df_out

        # return dictionary of all dataframes keyed by extract config paths
        return output
