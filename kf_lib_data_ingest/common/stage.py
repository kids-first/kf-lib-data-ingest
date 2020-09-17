import logging
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import wraps

from kf_lib_data_ingest.common.concept_schema import concept_attr_from
from kf_lib_data_ingest.common.io import read_json, write_json


class IngestStage(ABC):
    def __init__(self, ingest_output_dir=None):
        self.ingest_output_dir = ingest_output_dir
        if self.ingest_output_dir:
            self.stage_cache_dir = os.path.join(
                self.ingest_output_dir, type(self).__name__
            )
            os.makedirs(self.stage_cache_dir, exist_ok=True)
        else:
            self.stage_cache_dir = None

        self.logger = logging.getLogger(type(self).__name__)

    @property
    def stage_type(self):
        """
        Collapse stage subtypes to their bases for storage
        (e.g. GuidedTransformStage -> TransformStage)
        """
        stage_type = type(self)
        while stage_type.__base__ != IngestStage:
            stage_type = stage_type.__base__

        return stage_type

    def read_output(self):
        """
        Read the stage's previously written output from the output directory,
        stage_cache_dir. If stage_cache_dir is not defined or does not exist
        raise FileNotFoundError. Otherwise call the private _read_output method
        which is expected to be implemented by subclasses.

        :return: the output produced by _read_output (defined by sublcasses)
        """
        if not (self.stage_cache_dir and os.path.isdir(self.stage_cache_dir)):
            raise FileNotFoundError(
                f"Error reading {type(self).__name__} "
                f"output. The output directory: "
                f'"{self.stage_cache_dir}" does not exist'
            )
        else:
            return self._read_output()

    def write_output(self, output):
        """
        Write stage's output to the stage's output directory if it is
        defined. Call the private _write_output method which is expected to be
        implemented by subclasses.
        """
        if self.stage_cache_dir:
            self.logger.info(f"Writing {type(self).__name__} output")
            self._write_output(output)
            self.logger.info(f"Done writing {type(self).__name__} output")

    @abstractmethod
    def _run(self, *args, **kwargs):
        pass

    @abstractmethod
    def _validate_run_parameters(self, *args, **kwargs):
        # Subclasses should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        pass

    @abstractmethod
    def _write_output(self, output):
        """
        An ingest stage is responsible for serializing the data that is
        produced at the end of stage run.

        :param output: some data structure that needs to be serialized
        """
        pass

    @abstractmethod
    def _read_output(self):
        """
        An ingest stage is responsible for deserializing the data that it
        previously produced at the end of stage run

        :param serialized_output: a serialized representation of all of the
            data that this stage produces
        :type serialized_output: string
        :return: some data structure equal to what this stage produces
        """
        pass

    def _log_run(func):
        """
        Decorator to log the ingest stage's run

        Log begin, end and time elapsed
        """

        @wraps(func)
        def wrapper(instance, *args, **kwargs):
            logger = instance.logger

            # Get the inges stage name
            stage_name = instance.__class__.__name__

            # Log start run
            logger.info("BEGIN {}".format(stage_name))

            # Run the stage
            start = time.time()
            r = func(instance, *args, **kwargs)
            end = time.time()

            # Log end run
            delta_sec = end - start
            min, sec = divmod(delta_sec, 60)
            hour, min = divmod(min, 60)
            time_string = "Time elapsed: Sec: {} Min: {} Hours: {}".format(
                sec, min, hour
            )
            logger.info("END {}. {}".format(stage_name, time_string))

            return r

        return wrapper

    def _concept_discovery_filepath(self):
        """
        Location of stage run output's discovered counts file
        """
        return os.path.join(
            self.ingest_output_dir,
            self.stage_type.__name__ + "_concept_discovery.json",
        )

    def write_concept_counts(self):
        """
        Write concept discovery dict to disk
        """
        fp = self._concept_discovery_filepath()
        self.logger.debug(f"Writing discovered counts to {fp}")
        write_json(self.concept_discovery_dict, fp)

    def read_concept_counts(self):
        """
        Read concept discovery dict from disk
        """
        fp = self._concept_discovery_filepath()
        self.logger.debug(f"Reading discovered counts from {fp}")
        self.concept_discovery_dict = read_json(fp)

    @_log_run
    def run(self, *args, **kwargs):
        # Validate run parameters
        self._validate_run_parameters(*args, **kwargs)

        # Execute data ingest stage
        output = self._run(*args, **kwargs)

        # Write output of stage to disk
        self.write_output(output)

        # Write data for accounting to disk
        self.logger.info("Counting what we got")
        self.concept_discovery_dict = self._postrun_concept_discovery(output)
        self.logger.info("Done counting what we got")
        if self.concept_discovery_dict:
            # Undefault any defaultdicts. Defaultdicts are slightly dangerous
            # to pass downstream to someone else's custom test code where they
            # might accidentally add things while looking for keys.
            self.concept_discovery_dict = {
                k: dict(v)
                for k, v in self.concept_discovery_dict.items()
                if v is not None
            }
            self.write_concept_counts()
        return output

    def _postrun_concept_discovery(self, df_dict):
        """
        Builds a dict which stores all unique standard concept attributes (e.g.
        each PARTICIPANT.ID) found in the stage output mapped to lists of the
        places they appear

        dict template
        {
            'sources': {
                a_key: {  # e.g. PARTICIPANT.ID
                    a1: [f1, f2],  # e.g. PARTICIPANT.ID==a1 in files f1 & f2
                    ...
                },
                ...
            }
        }

        :param df_dict: a dict of DataFrames returned by the _run() method
        :return: a dict where concept values map to a list of the sources
        containing them
        :rtype: dict
        """
        sources = defaultdict(lambda: defaultdict(set))
        # Skip columns which might be set artificially
        skip = ["VISIBLE"]
        for df_name, df in df_dict.items():
            cols = [c for c in df.columns if concept_attr_from(c) not in skip]
            self.logger.info(
                f"Doing concept discovery for DataFrame {df_name} in "
                f"{type(self).__name__} output"
            )
            for key in cols:
                sk = sources[key]
                for val in df[key]:
                    sk[val].add(df_name)

        return {"sources": sources}
