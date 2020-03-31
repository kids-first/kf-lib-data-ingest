"""
Module for loading the transform output into the dataservice. It converts the
merged source data into complete message payloads according to a given API
specification, and then sends those messages to the target server.
"""
import concurrent.futures
import json
import logging
import os
from collections import defaultdict
from pprint import pformat
from threading import Lock, current_thread, main_thread
from urllib.parse import urlparse

from pandas import DataFrame
from sqlite3worker import Sqlite3Worker, sqlite3worker

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import multisplit, snake_case
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.config import DEFAULT_ID_CACHE_FILENAME
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)

sqlite3worker.LOGGER.setLevel(logging.WARNING)
count_lock = Lock()


class LoadStage(IngestStage):
    def __init__(
        self,
        target_api_config_path,
        target_url,
        entities_to_load,
        study_id,
        cache_dir=None,
        use_async=False,
        dry_run=False,
        resume_from=None,
    ):
        """
        :param target_api_config_path: path to the target service API config
        :type target_api_config_path: str
        :param target_url: URL for the target service
        :type target_url: str
        :param entities_to_load: set of which types of entities to load
        :type entities_to_load: list
        :param study_id: target ID of the study being loaded
        :type study_id: str
        :param cache_dir: where to find the ID cache, defaults to None
        :type cache_dir: str, optional
        :param use_async: use asynchronous networking, defaults to False
        :type use_async: bool, optional
        :param dry_run: don't actually transmit, defaults to False
        :type dry_run: bool, optional
        :param resume_from: Dry run until the designated target ID is seen, and
            then switch to normal loading. Does not overrule the dry_run flag.
            Value may be a full ID or an initial substring (e.g. 'BS', 'BS_')
        :type resume_from: str, optional
        """
        super().__init__(cache_dir)
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self._validate_entities(
            entities_to_load,
            f"Your ingest package config says to load invalid entities:",
        )
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.dry_run = dry_run
        self.resume_from = resume_from
        self.study_id = study_id
        self.use_async = use_async

        target = urlparse(target_url).netloc or urlparse(target_url).path
        self.uid_cache_filepath = os.path.join(
            cache_dir or os.getcwd(),
            #  Every target gets its own cache because they don't share UIDs
            "_".join(multisplit(target, [":", "/"]))
            #  Every study gets its own cache to compartmentalize internal IDs
            + "_" + study_id + "_" + DEFAULT_ID_CACHE_FILENAME,
        )

        if not os.path.isfile(self.uid_cache_filepath):
            self.logger.info(
                "Target UID cache file not found so a new one will be created:"
                f" {self.uid_cache_filepath}"
            )

        # Two-stage (RAM + disk) cache
        self.uid_cache = defaultdict(dict)
        self.uid_cache_db = Sqlite3Worker(self.uid_cache_filepath)

    def _validate_entities(self, entities_to_load, msg):
        """
        Validate that all entities in entities_to_load are one of the
        target concepts specified in the target_api_config.all_targets
        """
        target_names = [t.__name__ for t in self.target_api_config.all_targets]
        lower_names = [n.lower() for n in target_names]
        snake_names = [snake_case(name) for name in target_names]
        invalid_ents = [
            ent
            for ent in entities_to_load
            if (ent not in self.target_api_config.all_targets)
            and (ent not in target_names + ["default"])
            and (ent not in lower_names)
            and (ent not in snake_names)
        ]

        if invalid_ents:
            raise ConfigValidationError(
                f"{msg} "
                f"{pformat(invalid_ents)}. "
                "Valid entities must be one of the target concepts: "
                f"{pformat(target_names)} "
                f"specified in {self.target_api_config.config_filepath}"
            )

    def _validate_run_parameters(self, df_dict):
        """
        Validate the parameters being passed into the _run method. This method
        gets executed before the body of _run is executed.

        :param df_dict: a dict of DataFrames, keyed by target concepts defined
        in the target_api_config
        :type df_dict: dict
        """

        try:
            assert_safe_type(df_dict, dict)
            assert_all_safe_type(df_dict.values(), DataFrame)
        except TypeError as e:
            raise InvalidIngestStageParameters from e

        self._validate_entities(
            df_dict.keys(), "Your transform module output has invalid keys:"
        )

    def _prime_uid_cache(self, entity_type):
        """
        Make sure that the backing cache database table exists and that the RAM
        store is populated.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        """
        if entity_type not in self.uid_cache:
            # Create table in DB first if necessary
            self.uid_cache_db.execute(
                f'CREATE TABLE IF NOT EXISTS "{entity_type}"'
                " (unique_id TEXT PRIMARY KEY, target_id TEXT);"
            )
            # Populate RAM cache from DB
            for unique_id, target_id in self.uid_cache_db.execute(
                f'SELECT unique_id, target_id FROM "{entity_type}";'
            ):
                self.uid_cache[entity_type][unique_id] = target_id

    def _get_target_id(self, entity_type, entity_id):
        """
        Retrieve the target service ID for a given source unique ID.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_id: source unique ID for this entity
        :type entity_id: str
        """
        self._prime_uid_cache(entity_type)
        return self.uid_cache[entity_type].get(entity_id)

    def _store_target_id(self, entity_type, entity_id, target_id):
        """
        Cache the relationship between a source unique ID and its corresponding
        target service ID.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_id: source unique ID for this entity
        :type entity_id: str
        :param target_id: target service ID for this entity
        :type target_id: str
        """
        self._prime_uid_cache(entity_type)
        if self.uid_cache[entity_type].get(entity_id) != target_id:
            self.uid_cache[entity_type][entity_id] = target_id
            self.uid_cache_db.execute(
                f'INSERT OR REPLACE INTO "{entity_type}"'
                " (unique_id, target_id)"
                " VALUES (?,?);",
                (entity_id, target_id),
            )

    def _read_output(self):
        pass  # TODO

    def _write_output(self, output):
        pass  # TODO

    def _load_entity(self, entity_class, body, unique_key):
        """
        Prepare a single entity for submission to the target service.
        """
        if current_thread() is not main_thread():
            current_thread().name = f"{entity_class.__name__} {unique_key}"

        if self.resume_from:
            target_id = self._get_target_id(entity_class.__name__, unique_key)
            if not target_id:
                raise InvalidIngestStageParameters(
                    "Use of the resume_from flag requires having already"
                    " cached target IDs for all prior entities. The resume"
                    " target has not yet been reached, and no cached ID"
                    f" was found for this entity body:\n{pformat(body)}"
                )
            elif target_id.startswith(self.resume_from):
                self.logger.info(
                    f"Found resume target '{self.resume_from}'. Resuming"
                    " normal load."
                )
                self.dry_run = False
                self.resume_from = None

        if self.dry_run:
            target_id = self._get_target_id(entity_class.__name__, unique_key)
            if target_id:
                req_method = "UPDATE"
                id_str = f"({target_id})"
            else:
                req_method = "ADD"
                id_str = f"({unique_key})"

            self.logger.debug(f"Request body preview:\n{pformat(body)}")
            done_msg = (
                f"DRY RUN - {req_method} {entity_class.__name__} {id_str}"
            )
        else:
            # send to the target service
            target_id = self.target_api_config.submit(
                self.target_url, entity_class, body
            )

            # cache source_ID:target_ID lookup
            self._store_target_id(entity_class.__name__, unique_key, target_id)

            done_msg = (
                f"Loaded {entity_class.__name__} {unique_key} --> {target_id}"
            )

        # log action
        with count_lock:
            self.sent_messages.append(
                {
                    "host": self.target_url,
                    "type": entity_class.__name__,
                    "body": body,
                }
            )
            self.counts[entity_class.__name__] += 1
            self.logger.info(
                done_msg + f" (#{self.counts[entity_class.__name__]})"
            )

    def _postrun_concept_discovery(self, run_output):
        pass  # TODO

    def _run(self, transform_output):
        """
        Load Stage internal entry point. Called by IngestStage.run

        :param transform_output: Output data structure from the Transform stage
        :type transform_output: dict
        """
        self.counts = {}
        self.seen_entities = defaultdict(set)

        if self.dry_run:
            self.logger.info(
                "DRY RUN mode is ON. No entities will be loaded into the "
                "target service."
            )
            self.resume_from = None
        elif self.resume_from:
            self.logger.info(
                f"Will dry run until '{self.resume_from}' and then resume"
                " loading from that entity."
            )
            self.dry_run = True

        # Loop through all target concepts
        self.sent_messages = []
        try:
            for entity_class in self.target_api_config.all_targets:
                if not any(
                    x in self.entities_to_load
                    for x in [
                        entity_class,
                        entity_class.__name__,
                        entity_class.__name__.lower(),
                        snake_case(entity_class.__name__),
                    ]
                ):
                    self.logger.info(
                        f"Skipping load of {entity_class.__name__}"
                    )
                    continue

                self.logger.info(f"Begin loading {entity_class.__name__}")

                # we can be flexible about what we accept from transform
                t_key = None
                if entity_class in transform_output:
                    t_key = entity_class
                elif entity_class.__name__ in transform_output:
                    t_key = entity_class.__name__
                elif entity_class.__name__.lower() in transform_output:
                    t_key = entity_class.__name__.lower()
                elif snake_case(entity_class.__name__) in transform_output:
                    t_key = snake_case(entity_class.__name__)

                if t_key is None:
                    t_key = "default"

                if isinstance(transform_output[t_key], DataFrame):
                    # guarantee existence of the study ID column
                    transform_output[t_key][CONCEPT.STUDY.ID] = self.study_id
                    # convert df to list of dicts
                    transform_output[t_key] = transform_output[t_key].to_dict(
                        "records"
                    )

                self.counts[entity_class.__name__] = 0

                if self.use_async:
                    ex = concurrent.futures.ThreadPoolExecutor()
                    futures = []

                for row in transform_output[t_key]:
                    try:
                        unique_key = entity_class.build_key(row)
                    except Exception:
                        # no new key, no new entity
                        continue

                    if (not unique_key) or (
                        unique_key in self.seen_entities[entity_class.__name__]
                    ):
                        # no new key, no new entity
                        continue

                    self.seen_entities[entity_class.__name__].add(unique_key)

                    payload = entity_class.build_entity(
                        row, unique_key, self._get_target_id
                    )

                    if self.use_async and not self.resume_from:
                        futures.append(
                            ex.submit(
                                self._load_entity,
                                entity_class,
                                payload,
                                unique_key,
                            )
                        )
                    else:
                        self._load_entity(entity_class, payload, unique_key)

                if self.use_async:
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
                    ex.shutdown()

                self.logger.info(f"End loading {entity_class.__name__}")
        finally:
            json_out = os.path.join(self.stage_cache_dir, "SentMessages.json")
            with open(json_out, "w") as jo:
                json.dump(self.sent_messages, jo, indent=2)

        if self.resume_from:
            self.logger.warning(
                f"⚠️ Could not find resume_from target '{self.resume_from}'! "
                "Nothing was actually loaded into the target service."
            )
