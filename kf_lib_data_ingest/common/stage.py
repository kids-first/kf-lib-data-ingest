import logging
import os
import time
from abc import ABC, abstractmethod
from functools import wraps


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

    def _validation_filepath(self):
        """
        Location of stage run output's validation
        """
        # TODO
        pass

    def write_validation(self):
        """
        Write validation to disk
        """
        # TODO
        pass

    def read_validation(self):
        """
        Read validation from disk
        """
        # TODO
        pass

    @_log_run
    def run(self, *args, **kwargs):
        # Validate run parameters
        self._validate_run_parameters(*args, **kwargs)

        # Execute data ingest stage
        output = self._run(*args, **kwargs)

        # Write output of stage to disk
        self.write_output(output)

        # Write data for validation to disk
        # TODO

        return output

    def _postrun_validation(self, df_dict):
        """
        Post run validation

        :param df_dict: a dict of DataFrames returned by the _run() method
        :return: a dict where concept values map to a list of the sources
        containing them
        :rtype: dict
        """
        # TODO
        pass
