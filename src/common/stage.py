import time
import logging
from functools import wraps

from abc import (
    ABC,
    abstractmethod
)


class IngestStage(ABC):

    operation = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def write_output(self, output, output_dir, overwrite):
        # TODO An ingest stage is responsible for writing out the data that it
        # produces via its run method
        pass

    @classmethod
    @abstractmethod
    def read_output(self, output_dir):
        # TODO An ingest stage is responsible for reading the data it wrote out
        # from its run method
        pass

    @abstractmethod
    def _run(self, *args, **kwargs):
        pass

    @abstractmethod
    def _validate_run_parameters(self, *args, **kwargs):
        # Subclasses should raise a InvalidIngestStageParameters if any
        # parameters are missing.
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
