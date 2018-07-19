from abc import (
    ABC,
    abstractmethod
)

from etl.configuration.dataset_ingest_config import DatasetIngestConfig


class IngestStage(ABC):

    def __init__(self, dataset_ingest_config_path):
        self.dataset_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)

    @abstractmethod
    def _run(self, *args, **kwargs):
        pass

    @abstractmethod
    def _validate_run_parameters(*args, **kwargs):
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

    def run(self, *args, **kwargs):
        # Validate run parameters
        self._validate_run_parameters(*args, **kwargs)

        # Execute data ingest stage
        output = self._run(*args, **kwargs)

        # Write output of stage to disk
        self._write_output(output)

        return output
