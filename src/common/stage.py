import time
import logging
from functools import wraps

from abc import (
    ABC,
    abstractmethod
)


class IngestStage(ABC):

    def __init__(self):
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
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    @abstractmethod
    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _construct_output_filepath(self):
        # Construct the filepath of the output using the study's config
        # directory which is basename of
        # self.dataset_ingest_config.config_filepath
        # Store in
        # <study config dir path>/output_cache/<ingest stage class name>
        pass

    def _read_output(self):
        filepath = self._construct_output_filepath()
        pass  # TODO

    def _write_output(self, output):
        filepath = self._construct_output_filepath()
        serialized = self._serialize_output(output)
        # TODO
        # Write serialized output to file

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
        # self._write_output(output)

        return output
