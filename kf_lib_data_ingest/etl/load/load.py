"""
Module for loading the transform output into the dataservice.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)

# TODO
# REMEMBER THIS!
# Translate identifiers of entities to kf ids and attach them as foreign
# keys.
# Cache the return from every load.
# Load according to the dataservice api endpoints.


class LoadStage(IngestStage):
    def __init__(
        self, target_api_config_path, target_url, use_async=False,
        entities_to_load=None, ingest_output_dir=None, write_output=False
    ):
        super().__init__(ingest_output_dir=None, write_output=False)
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.use_async = use_async

    def _read_output(self):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _write_output(self, output):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _load(self, thing):
        # TODO: put the thing into the server (does the sending)
        pass

    def _validate_run_parameters(self, target_entities):
        # Should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        # raise InvalidIngestStageParameters
        # TODO
        # Check that standard model is an instance of the tbd standard model
        # class
        pass

    def _run(self, target_entities):
        # TODO: revisit maybe?
        for entity_type, entities in target_entities.items():
            for entity in entities:
                self._load(entity)
