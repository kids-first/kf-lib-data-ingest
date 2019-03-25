"""
Module for loading the transform output into the dataservice.
"""
import os
from abc import abstractmethod
from urllib.parse import urlparse

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import read_json
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.config import DEFAULT_ID_CACHE_FILENAME
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
        self, target_api_config_path, target_url, entities_to_load,
        uid_cache_dir=None, use_async=False
    ):
        super().__init__()
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.uid_cache_filepath = os.path.join(
            uid_cache_dir or os.getcwd(),
            urlparse(target_url).netloc + '_' + DEFAULT_ID_CACHE_FILENAME
            #  Every target gets its own cache because they don't share UIDs
        )
        try:
            self.uid_cache = read_json(self.uid_cache_filepath)
        except FileNotFoundError as e:
            self.logger.warning(
                f'Target UID cache file not found: {self.uid_cache_filepath}'
            )
            self.uid_cache = {}
        self.use_async = use_async

    def _unique_key_to_cached_target_id(self, entity_type, entity_unique_key):
        return self.uid_cache.get(entity_type, {}).get(entity_unique_key)

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
