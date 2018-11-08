import logging
import os
import time
from abc import ABC, abstractmethod
from functools import wraps


class IngestStage(ABC):

    def __init__(self, study_config_dir=None):
        self.study_config_dir = study_config_dir
        self.logger = logging.getLogger(type(self).__name__)

    @abstractmethod
    def _run(self, *args, **kwargs):
        pass

    @abstractmethod
    def _validate_run_parameters(self, *args, **kwargs):
        # Subclasses should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        pass

    @abstractmethod
    def _serialize_output(self, output):
        """
        An ingest stage is responsible for serializing the data that is
        produced at the end of stage run.

        :param output: some data structure that needs to be serialized
        """
        pass

    @abstractmethod
    def _deserialize_output(self, serialized_output):
        """
        An ingest stage is responsible for deserializing the data that it
        previously produced at the end of stage run

        :param serialized_output: a serialized representation of all of the
            data that this stage produces
        :type serialized_output: string
        :return: some data structure equal to what this stage produces
        """
        pass

    def _construct_output_filepath(self):
        """
        Construct the filepath of the output.
        Something like:
            <study config dir path>/output_cache/<ingest stage class name>.xxx

        :return: file location to put/get serialized output for this stage
        :rtype: string
        """
        return os.path.join(
            self.study_config_dir, 'output_cache',
            type(self).__name__ + '_cache.txt'
        ) if self.study_config_dir else None

    def _read_output(self):
        """
        Deserialize from a file.
        """
        filepath = self._construct_output_filepath()
        with open(filepath) as fp:
            return self._deserialize_output(fp.read())

    def _write_output(self, output):
        """
        Serialize to a file.
        """
        filepath = self._construct_output_filepath()
        serialized = self._serialize_output(output)
        if filepath and serialized:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as fp:
                fp.write(serialized)

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
            logger.info('BEGIN {}'.format(stage_name))

            # Run the stage
            start = time.time()
            r = func(instance, *args, **kwargs)
            end = time.time()

            # Log end run
            delta_sec = end - start
            min, sec = divmod(delta_sec, 60)
            hour, min = divmod(min, 60)
            time_string = ("Time elapsed: Sec: {} Min: {} Hours: {}"
                           .format(sec, min, hour))
            logger.info('END {}. {}'.format(stage_name, time_string))

            return r
        return wrapper

    @_log_run
    def run(self, *args, **kwargs):
        # Validate run parameters
        self._validate_run_parameters(*args, **kwargs)

        # Execute data ingest stage
        output = self._run(*args, **kwargs)

        # Write output of stage to disk
        self._write_output(output)

        return output
