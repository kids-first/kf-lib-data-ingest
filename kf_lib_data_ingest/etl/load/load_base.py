"""
Module for loading the transform output into the dataservice. It converts the
merged source data into complete message payloads according to a given API
specification, and then sends those messages to the target server.
"""

import concurrent.futures
import json
import os
import sqlite3
from collections import defaultdict
from pprint import pformat
from threading import Lock, current_thread, main_thread
from urllib.parse import urlparse

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import multisplit
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
from pandas import DataFrame

count_lock = Lock()
cache_lock = Lock()


class LoadStageBase(IngestStage):
    def __init__(
        self,
        target_api_config_path,
        target_url,
        entities_to_load,
        project_id,
        cache_dir=None,
        use_async=False,
        dry_run=False,
        resume_from=None,
        clear_cache=False,
    ):
        """
        :param target_api_config_path: path to the target service API config
        :type target_api_config_path: str
        :param target_url: URL for the target service
        :type target_url: str
        :param entities_to_load: set of which types of entities to load
        :type entities_to_load: list
        :param project_id: unique ID of the project being loaded
        :type project_id: str
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
        :param clear_cache: Clear the identifier cache file before loading,
            defaults to False. Equivalent to deleting the file manually. Ignored
            when using resume_from, because that needs the cache to be effective.
        :type clear_cache: bool, optional
        """
        super().__init__(cache_dir or os.getcwd())
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self._validate_entities(
            entities_to_load,
            "Your ingest package config says to load invalid entities:",
        )
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.dry_run = dry_run
        self.resume_from = resume_from
        self.project_id = project_id
        self.use_async = use_async
        self._dry_id = 0

        self.uid_cache_filepath = os.path.join(
            self.stage_cache_dir,
            #  Every target gets its own cache because they don't share UIDs
            self._clean_name(target_url)
            #  Every project gets its own cache to compartmentalize internal IDs
            + "_" + project_id + "_" + DEFAULT_ID_CACHE_FILENAME,
        )

        if not os.path.isfile(self.uid_cache_filepath):
            self.logger.info(
                "Target identifier cache file not found so a new one will be created:"
                f" {self.uid_cache_filepath}"
            )
        elif clear_cache and not self.resume_from:
            os.remove(self.uid_cache_filepath)
            self.logger.info(
                "Not resuming a previous run, so the identifier cache file at "
                f"{self.uid_cache_filepath} has been cleared."
            )

        # Two-stage (RAM + disk) cache
        self.uid_cache = defaultdict(dict)
        self.uid_cache_db = sqlite3.connect(
            self.uid_cache_filepath,
            isolation_level=None,
            check_same_thread=False,
        )

    def _clean_name(self, target_url):
        target = urlparse(target_url).netloc or urlparse(target_url).path
        return "_".join(multisplit(target, [":", "/"]))

    def _validate_entities(self, entities_to_load, msg):
        """
        Validate that all entities in entities_to_load are one of the
        target concepts specified in the target_api_config.all_targets
        """
        target_names = {
            t.class_name for t in self.target_api_config.all_targets
        }
        invalid_ents = set(entities_to_load) - target_names
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
            set(df_dict.keys()) - {"default"},
            "Your transform module output has invalid keys:",
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

    def _get_target_id_from_key(self, entity_type, entity_key):
        """
        Retrieve the target service ID for a given source unique key.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_key: source unique key for this entity
        :type entity_key: str
        """
        with cache_lock:
            self._prime_uid_cache(entity_type)
            return self.uid_cache[entity_type].get(entity_key)

    def _store_target_id_for_key(
        self, entity_type, entity_key, target_id, no_db
    ):
        """
        Cache the relationship between a source unique key and its corresponding
        target service ID.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param entity_key: source unique key for this entity
        :type entity_key: str
        :param target_id: target service ID for this entity
        :type target_id: str
        :param no_db: only store in the RAM cache, not in the db
        :type no_db: bool
        """
        with cache_lock:
            self._prime_uid_cache(entity_type)
            if self.uid_cache[entity_type].get(entity_key) != target_id:
                self.uid_cache[entity_type][entity_key] = target_id
                if not no_db:
                    self.uid_cache_db.execute(
                        f'INSERT OR REPLACE INTO "{entity_type}"'
                        " (unique_id, target_id)"
                        " VALUES (?,?);",
                        (entity_key, target_id),
                    )

    def _get_target_id_from_record(self, entity_class, record):
        """
        Find the target service ID for the given record and entity class.

        :param entity_class: one of the classes contained in the all_targets list
        :type entity_class: class
        :param record: a record of extracted data
        :type record: dict
        :return: the target service ID
        :rtype: str
        """
        raise NotImplementedError()

    def _do_target_submit(self, entity_class, body):
        """Shim for target API submission across loader versions"""
        raise NotImplementedError()

    def _do_target_get_key(self, entity_class, record):
        """Shim for target API key building across loader versions"""
        raise NotImplementedError()

    def _do_target_get_entity(self, entity_class, record, keystring):
        """Shim for target API entity building across loader versions"""
        raise NotImplementedError()

    def _read_output(self):
        pass  # TODO

    def _write_output(self, output):
        pass  # TODO

    def _load_entity(self, entity_class, record):
        """
        Prepare a single entity for submission to the target service.
        """
        try:
            key_components = self._do_target_get_key(entity_class, record)
        except Exception as e:
            # no new key, no new entity
            if isinstance(e, KeyError):
                self.logger.warning(f"❌ Key {str(e)} not found")
            elif isinstance(e, ValueError) and "Missing required" in str(e):
                self.logger.warning(
                    "❌ One or more of the required key components for looking"
                    f" up {entity_class.class_name} is null. Check out"
                    f" {entity_class.class_name} definition in the target API"
                    " plugin to see what is required"
                )
            key_components = None

        if not key_components:
            self.logger.warning(
                f"⚠️  Skip {entity_class.class_name}. Missing key components. "
                f"Failed to construct unique key from record:"
                f"\n{pformat(record)}"
            )
            return

        unique_key = str(key_components)
        if unique_key in self.seen_entities[entity_class.class_name]:
            # no new key, no new entity
            self.logger.debug(
                f"Skip {entity_class.class_name}. Duplicate record found in "
                f"data:\n{record}"
            )
            return

        self.seen_entities[entity_class.class_name].add(unique_key)

        target_id = self._get_target_id_from_record(entity_class, record)
        method = "UPDATE" if target_id else "CREATE"

        try:
            body = self._do_target_get_entity(entity_class, record, unique_key)
        except Exception:
            self.logger.info(
                f"Failed to build {entity_class.class_name} from record {record}"
            )
            raise

        if current_thread() is not main_thread():
            current_thread().name = f"{entity_class.class_name} {unique_key}"

        if self.resume_from:
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

        msg = f"{method} {entity_class.class_name} ({unique_key})"
        if target_id:
            msg = f"{msg} [{target_id}]"

        if self.dry_run:
            self.logger.debug(f"Request body preview:\n{pformat(body)}")
            msg = f"DRY RUN - {msg}"
            if not target_id:
                self._dry_id += 1
                target_id = f"DRY_{entity_class.class_name}_{self._dry_id}"
        else:
            # send to the target service
            target_id = self._do_target_submit(entity_class, body)
            msg = f"{msg} --> {target_id}"

        # cache source_ID:target_ID lookup
        self._store_target_id_for_key(
            entity_class.class_name, unique_key, target_id, self.dry_run
        )

        # log action
        with count_lock:
            self.sent_messages.append(
                {
                    "type": entity_class.class_name,
                    "method": method,
                    "body": body,
                }
            )
            self.counts[entity_class.class_name][method] += 1
            self.logger.info(
                f"{msg} (#{sum(self.counts[entity_class.class_name].values())})"
            )

    def _postrun_validation(self, validation_mode=None, report_kwargs={}):
        # Override implemented base class method because we don't need to
        # do any validation on this stage's output
        pass

    def _run(self, transform_output):
        """
        Load Stage internal entry point. Called by IngestStage.run

        :param transform_output: Output data structure from the Transform stage
        :type transform_output: dict
        """
        self.counts = defaultdict(dict)
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
                if entity_class.class_name not in self.entities_to_load:
                    self.logger.info(
                        f"Skipping load of {entity_class.class_name}. Not "
                        "included in ingest package config."
                    )
                    continue

                self.logger.info(f"Begin loading {entity_class.class_name}")

                if entity_class.class_name in transform_output:
                    t_key = entity_class.class_name
                else:
                    t_key = "default"

                # convert df to list of dicts
                transformed_records = transform_output[t_key].to_dict(
                    "records")

                if hasattr(entity_class, "transform_records_list"):
                    transformed_records = entity_class.transform_records_list(
                        transformed_records
                    )

                # guarantee existence of the project unique key column
                for r in transformed_records:
                    r[CONCEPT.PROJECT.ID] = self.project_id

                self.counts[entity_class.class_name]["CREATE"] = 0
                self.counts[entity_class.class_name]["UPDATE"] = 0

                if self.use_async:
                    ex = concurrent.futures.ThreadPoolExecutor()
                    futures = []

                self.logger.info(
                    f"Reading {len(transformed_records)} rows in '{t_key}' table."
                )
                for record in transformed_records:
                    if self.use_async and not self.resume_from:
                        futures.append(
                            ex.submit(self._load_entity, entity_class, record)
                        )
                    else:
                        self._load_entity(entity_class, record)

                if self.use_async:
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
                    ex.shutdown()

                self.logger.info(f"End loading {entity_class.class_name}")
        finally:
            target = self._clean_name(self.target_url)
            json_out = os.path.join(
                self.stage_cache_dir, f"SentMessages_{target}.json"
            )
            with open(json_out, "w") as jo:
                json.dump(self.sent_messages, jo, indent=2)

        if self.resume_from:
            self.logger.warning(
                f"⚠️ Could not find resume_from target '{self.resume_from}'! "
                "Nothing was actually loaded into the target service."
            )

        self.logger.info(f"Load Summary:\n{pformat(dict(self.counts))}")
