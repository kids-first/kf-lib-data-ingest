"""
Module for loading the transform output into the dataservice.
"""
from abc import abstractmethod

from src.etl.common.errors import InvalidIngestStageParameters
from src.etl.configuration.base_config import YamlConfig
from src.etl.common.stage import IngestStage


class Loader(IngestStage):
    def __init__(
        self, target_url, use_async, study_config_path, target_schema_path
    ):
        super().__init__(study_config_path)
        # use study config to get the set of entities we want to load for the
        # study
        self.target_schema = YamlConfig(target_schema_path).contents
        self.target_url = target_url
        self.use_async = use_async
        # TODO: finish

    def _validate_run_parameters(*args, **kwargs):
        # Should raise a InvalidIngestStageParameters if any
        # parameters are missing.
        # TODO
        raise InvalidIngestStageParameters

    def _serialize_output(self, output):
        # An ingest stage is responsible for serializing the data that is
        # produced at the end of stage run
        pass  # TODO

    def _deserialize_output(self, filepath):
        # An ingest stage is responsible for deserializing the data that it
        # previously produced at the end of stage run
        pass  # TODO

    def _run(self, standard_model):
        # TODO: revisit maybe?
        for entity in self._adapt(standard_model):
            self._load(entity)

    def _adapt(self, standard_model):
        # TODO: will use the target schema and standard model to yield entities
        # to load into the dataservice
        pass

    @abstractmethod
    def _load(self, thing):
        # TODO: put the thing into the server (does the sending)
        pass


class KidsFirstDataserviceLoader(Loader):
    # TODO
    # Translate identifiers of entities to kf ids and attach them as foreign
    # keys.
    # Cache the return from every load.
    # Load according to the dataservice api endpoints.
    pass
