import logging
import os
import time
from abc import ABC, abstractmethod
from functools import wraps


class IngestStage(ABC):

    def __init__(self, ingest_output_dir=None):
        self.ingest_output_dir = ingest_output_dir
        if self.ingest_output_dir:
            self.stage_cache_dir = os.path.join(self.ingest_output_dir,
                                                type(self).__name__)
        else:
            self.stage_cache_dir = None

        self.logger = logging.getLogger(type(self).__name__)

    def read_output(self):
        """
        Read the stage's previously written output from the output directory,
        stage_cache_dir. If stage_cache_dir is not defined or does not exist
        raise FileNotFoundError. Otherwise call the private _read_output method
        which is expected to be implemented by subclasses.

        :returns: the output produced by _read_output (defined by sublcasses)
        """
        if not (self.stage_cache_dir and os.path.isdir(self.stage_cache_dir)):
            raise FileNotFoundError(f'Error reading {type(self).__name__} '
                                    f'output. The output directory: '
                                    f'"{self.stage_cache_dir}" does not exist')
        else:
            return self._read_output()

    def write_output(self, output):
        """
        Write stage's output to the stage's output directory if it is
        defined. Call the private _write_output method which is expected to be
        implemented by subclasses.
        """
        if self.stage_cache_dir:
            os.makedirs(self.stage_cache_dir, exist_ok=True)
            self._write_output(output)

    @abstractmethod
    def _run(self, *args, **kwargs):
        pass

    def _postrun_discovery(self, run_output):
        """
        Builds a dict which stores:
            - All unique standard concept attributes (e.g. each PARTICIPANT.ID)
            found in the stage output mapped to lists of the places they appear
            - All unique pairs of standard concept attributes found in the
            stage output (e.g. which BIOSPECIMEN.IDs are connected to which
            PARTICIPANT.IDs etc)

        dict template
        {
            'sources': {
                a_key: {  # e.g. PARTICIPANT.ID
                    a1: [f1, f2],  # e.g. PARTICIPANT.ID==a1 in files f1 & f2
                    ...
                },
                ...
            },
            'links': {
                a_key_b_key: { #  e.g. PARTICIPANT.ID and BIOSPECIMEN.ID
                    a1: [b1, b2], # Participant a1 linked to specimens b1 & b2
                    ...
                }
            }
        }

        :param run_output: the output returned by the _run() method
        :return: A dict where 1) concept values map to a list of the sources
        containing them and 2) concept values map to lists of linked concept
        values
        """
        return {'sources': None, 'links': None}

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
        self.write_output(output)

        output_tally = self._postrun_discovery(output)

        return output, output_tally
