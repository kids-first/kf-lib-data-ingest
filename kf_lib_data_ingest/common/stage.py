import logging
import os
import time
from abc import ABC, abstractmethod
from functools import wraps

from kf_lib_data_ingest.common.io import path_to_file_list, read_json
from kf_lib_data_ingest.validation.validation import (
    Validator,
    check_results,
    RESULTS_FILENAME,
    VALIDATION_OUTPUT_DIR,
)

BASIC_VALIDATION = "basic"
ADVANCED_VALIDATION = "advanced"


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
        self.validation_success = True
        self.validation_output_dir = os.path.join(
            self.stage_cache_dir or "",
            VALIDATION_OUTPUT_DIR,
        )

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
            # Read and evaluate cached validation results
            fp = self._validation_results_filepath()
            try:
                results = read_json(fp)
                self.validation_success = check_results(results)
            except FileNotFoundError:
                self.logger.info(
                    f"Validation results file: {fp} not found for "
                    f"stage: {type(self).__name__}"
                )

            # Read stage output
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

    def _validation_results_filepath(self):
        """
        Path to validation results file
        """
        return os.path.join(self.validation_output_dir, RESULTS_FILENAME)

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

    @_log_run
    def run(self, *args, **kwargs):
        # Validate run parameters
        vmode = kwargs.pop("validation_mode", None)
        report_kwargs = kwargs.pop("report_kwargs", {})
        assert vmode in [None, BASIC_VALIDATION, ADVANCED_VALIDATION]
        self._validate_run_parameters(*args, **kwargs)

        # Execute data ingest stage
        output = self._run(*args, **kwargs)

        # Write output of stage to disk
        self.write_output(output)

        # Run validation and write results to disk
        self._postrun_validation(
            validation_mode=vmode, report_kwargs=report_kwargs
        )

        return output

    def _postrun_validation(self, validation_mode=None, report_kwargs={}):
        """
        Post run validation.

        If validation_mode = None, do not run validation.

        If validation_mode = BASIC_VALIDATION, validation runs faster but is
        not as thorough.

        If validation_mode = ADVANCED_VALIDATION, validation takes into account
        implied relationships in the data and is able to resolve
        ambiguities or report the ambiguities if they cannot be resolved.

        For example, you have a file that relates participants and  specimens,
        and a file that relates participants and genomic files.
        This means your specimens have implied connections to their genomic
        files through the participants.

        In ADVANCED_VALIDATION mode, the validator may be able to resolve these
        implied connections and report that all specimens are validly linked to
        genomic files. In BASIC_VALIDATION mode, the validator will report
        that all specimens are missing links to genomic files.

        :param validation_mode: validation mode
        :return: whether or not validation passed
        :rtype: bool
        """
        if not validation_mode:
            return True

        self.logger.info(
            f"Running validation on {type(self).__name__} output files ..."
        )

        if validation_mode == BASIC_VALIDATION:
            include_implicit = False
        else:
            include_implicit = True

        self.validation_success = Validator(
            output_dir=os.path.dirname(self._validation_results_filepath()),
            init_logger=False,
        ).validate(
            path_to_file_list(self.stage_cache_dir, recursive=False),
            include_implicit=include_implicit,
            report_kwargs=report_kwargs,
        )
        if self.validation_success:
            self.logger.info(
                f"✅ {self.stage_type.__name__} passed validation!"
            )
        else:
            self.logger.info(
                f"❌ {self.stage_type.__name__} failed validation!"
            )
