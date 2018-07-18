"""
Module for loading the transform output into the dataservice.
"""
from abc import abstractmethod

from common.errors import InvalidIngestStageParameters
from common.stage import IngestStage
from etl.configuration.target_api_config import TargetAPIConfig

# TODO
# REMEMBER THIS!
# Translate identifiers of entities to kf ids and attach them as foreign
# keys.
# Cache the return from every load.
# Load according to the dataservice api endpoints.


class LoadStage(IngestStage):
    def __init__(
        self, target_api_config_path, target_url, use_async, entities_to_load
    ):
        # use study config to get the set of entities we want to load for the
        # study
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.use_async = use_async

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _adapt(self, standard_model):
        entity = {}
        # TODO: will use the target schema and standard model to yield entities
        # to load into the dataservice
        return entity

    def _load(self, thing):
        # TODO: put the thing into the server (does the sending)
        pass

    def _validate_run_parameters(self, standard_model):
        # Should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        # raise InvalidIngestStageParameters
        # TODO
        # Check that standard model is an instance of the tbd standard model
        # class
        pass

    def _run(self, standard_model):
        # TODO: revisit maybe?
        for entity in self._adapt(standard_model):
            self._load(entity)
