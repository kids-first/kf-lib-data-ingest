"""
Module for loading the transform output into the dataservice.
"""
import concurrent.futures
import logging
import os
from collections import defaultdict
from pprint import pformat
from threading import Lock, current_thread, main_thread
from urllib.parse import urlparse

import requests
from requests import RequestException
from sqlite3worker import Sqlite3Worker, sqlite3worker

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import multisplit
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.config import DEFAULT_ID_CACHE_FILENAME
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)
from kf_lib_data_ingest.network.utils import RetrySession

sqlite3worker.LOGGER.setLevel(logging.WARNING)
count_lock = Lock()


class LoadStage(IngestStage):
    def __init__(
        self,
        target_api_config_path,
        target_url,
        entities_to_load,
        study_id,
        uid_cache_dir=None,
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
        :param uid_cache_dir: where to find the ID cache, defaults to None
        :type uid_cache_dir: str, optional
        :param use_async: use asynchronous networking, defaults to False
        :type use_async: bool, optional
        :param dry_run: don't actually transmit, defaults to False
        :type dry_run: bool, optional
        :param resume_from: Dry run until the designated target ID is seen, and
            then switch to normal loading. Does not overrule the dry_run flag.
            Value may be a full ID or an initial substring (e.g. 'BS', 'BS_')
        :type resume_from: str, optional
        """
        super().__init__()
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.concept_targets = {
            v["standard_concept"].UNIQUE_KEY: k
            for k, v in self.target_api_config.target_concepts.items()
        }
        self.target_id_key = (
            self.target_api_config.contents.target_service_entity_id
        )
        self._validate_entities(entities_to_load)
        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.dry_run = dry_run
        self.resume_from = resume_from
        self.study_id = study_id
        self.totals = {}
        self.counts = {}

        target = urlparse(target_url).netloc or urlparse(target_url).path
        self.uid_cache_filepath = os.path.join(
            uid_cache_dir or os.getcwd(),
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

        self.entity_cache = dict()
        self.use_async = use_async

    def _validate_entities(self, entities_to_load):
        """
        Validate that all entities in entities_to_load are one of the
        target concepts specified in the target_api_config.target_concepts
        """
        assert_safe_type(entities_to_load, list)
        assert_all_safe_type(entities_to_load, str)

        invalid_ents = [
            ent
            for ent in entities_to_load
            if ent not in self.target_api_config.target_concepts
        ]
        if invalid_ents:
            valid_ents = list(self.target_api_config.target_concepts.keys())
            raise ValueError(
                f"Ingest package config has invalid entities: "
                f"{pformat(invalid_ents)} specified in `entities_to_load`. "
                "Valid entities must be one of the target concepts: "
                f"{pformat(valid_ents)} "
                f"specified in {self.target_api_config.config_filepath}"
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

    def _PATCH(self, endpoint, target_id, body):
        """
        Send a PATCH request to the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param target_id: which target service ID to patch
        :type target_id: str
        :param body: map between entity keys and values
        :type body: dict
        :return: PATCH request response
        :rtype: requests.Response
        """
        host = self.target_url
        return RetrySession().patch(
            url="/".join([v.strip("/") for v in [host, endpoint, target_id]]),
            json=body,
        )

    def _POST(self, endpoint, body):
        """
        Send a POST request to the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param body: map between entity keys and values
        :type body: dict
        :return: POST request response
        :rtype: requests.Response
        """
        host = self.target_url
        return RetrySession().post(
            url="/".join([v.strip("/") for v in [host, endpoint]]), json=body
        )

    def _GET(self, endpoint, body):
        """
        Send a GET request to the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param body: filter arguments keys and values
        :type body: dict
        :return: GET request response
        :rtype: requests.Response
        """
        host = self.target_url
        return RetrySession().get(
            url="/".join([v.strip("/") for v in [host, endpoint]]),
            params={k: v for k, v in body.items() if v is not None},
        )

    def _submit(self, entity_id, entity_type, endpoint, body):
        """
        Negotiate submitting the data for an entity to the target service.

        :param entity_id: source unique ID for this entity
        :type entity_id: str
        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param body: map between entity keys and values
        :type body: dict
        :raises RequestException: Unhandled response error from the server
        :return: The entity that the target service says was created or updated
        :rtype: dict
        """
        resp = None
        target_id = body.get(self.target_id_key)
        if target_id:
            self.logger.debug(f"Trying to PATCH {entity_type} {target_id}.")
            resp = self._PATCH(endpoint, target_id, body)
            if resp.status_code == 404:
                self.logger.debug(
                    f"Entity {entity_type} {target_id} not found in target "
                    "service."
                )
                resp = None

        if not resp:
            self.logger.debug(f"Trying to POST new {entity_type} {entity_id}.")
            resp = self._POST(endpoint, body)

        self.logger.debug(f"Request body: \n{pformat(body)}")

        if resp.status_code in {200, 201}:
            result = resp.json()["results"]
            self.logger.debug(f"Response body:\n{pformat(result)}")
            return result
        elif (resp.status_code == 400) and (
            "already exists" in resp.json()["_status"]["message"]
        ):
            # Our dataservice returns 400 if a relationship already exists
            # even though that's a silly thing to do.
            # See https://github.com/kids-first/kf-api-dataservice/issues/419
            extid = body.pop("external_id", None)
            resp = self._GET(endpoint, body)
            result = resp.json()["results"][0]
            self.logger.debug(f"Already exists:\n{pformat(result)}")
            if extid != result["external_id"]:
                self.logger.debug(f"Patching with new external_id: {extid}")
                self._PATCH(endpoint, result["kf_id"], {"external_id": extid})
            return result
        else:
            self.logger.debug(f"Response error:\n{pformat(resp.__dict__)}")
            raise RequestException(resp.text)

    def _apply_property_value_transformations(self, schema, payload):
        """
        For any properties in payload that have a value transform function
        specified in schema, apply the function to the value of the
        property in payload

        :param schema: target concept properties schema for the payload
        :type schema: dict
        :param payload: target concept instance
        :type payload: dict
        :returns: the modified payload with value transformations applied to it
        """
        for attribute, value in payload.items():
            payload[attribute] = value
            mapping = schema.get(attribute)
            if isinstance(mapping, tuple):
                payload[attribute] = mapping[-1](value)

        return payload

    def _load_entity(self, entity_type, endpoint, entity_id, body, links):
        """
        Prepare a single entity for submission to the target service.

        :param entity_type: the name of this type of entity
        :type entity_type: str
        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param entity_id: source unique ID for this entity
        :type entity_id: str
        :param body: map between entity keys and values
        :type body: dict
        :param links: map between entity keys and foreign key source unique IDs
        :type links: dict
        """
        if current_thread() is not main_thread():
            current_thread().name = f"{entity_type} {entity_id}"

        # populate target uid
        target_id_value = body.get(self.target_id_key)
        if target_id_value == constants.COMMON.NOT_REPORTED:
            target_id_value = self._get_target_id(entity_type, entity_id)
        body[self.target_id_key] = target_id_value

        # Remove elements with null values
        body = {k: v for k, v in body.items() if v is not None}

        # Apply property value transformations, if provided
        body = self._apply_property_value_transformations(
            self.target_api_config.target_concepts.get(entity_type).get(
                "properties"
            ),
            body,
        )
        # link cached foreign keys
        for link_dict in links:
            link_type = link_dict.pop("target_concept", None)
            for link_key, link_value in link_dict.items():
                if link_key == "study_id":
                    body[link_key] = self.study_id
                else:
                    target_link_value = body.get(link_key)
                    if target_link_value == constants.COMMON.NOT_REPORTED:
                        target_link_value = self._get_target_id(
                            link_type, link_value
                        )
                    body[link_key] = target_link_value

        if self.resume_from:
            tgt_id = body.get(self.target_id_key)
            if not tgt_id:
                raise InvalidIngestStageParameters(
                    "Use of the resume_from flag requires having already"
                    " cached target IDs for all prior entities. The resume"
                    " target has not yet been reached, and no cached ID"
                    f" was found for this entity body:\n{pformat(body)}"
                )
            elif tgt_id.startswith(self.resume_from):
                self.logger.info(
                    f"Found resume target '{self.resume_from}'. Resuming"
                    " normal load."
                )
                self.dry_run = False
                self.resume_from = None

        if self.dry_run:
            # Fake sending with fake primary/foreign keys
            tgt_id = body.get(self.target_id_key)
            body[
                self.target_id_key
            ] = f"source: {entity_id} --> target: {tgt_id}"
            for link_dict in links:
                for link_key, link_value in link_dict.items():
                    if not link_value:
                        link_value = body[link_key]
                    body[
                        link_key
                    ] = f"source: {link_value} --> target: {body[link_key]}"

            if tgt_id:
                req_method = "PATCH"
                id_str = f"{{{self.target_id_key}: {tgt_id}}}"
            else:
                req_method = "POST"
                id_str = f"({entity_id})"

            self.logger.debug(f"Request body preview:\n{pformat(body)}")
            done_msg = f"DRY RUN - {req_method} {endpoint} {id_str}"
        else:
            # send to the target service
            entity = self._submit(entity_id, entity_type, endpoint, body)

            # cache source_ID:target_ID lookup
            tgt_id = entity[self.target_id_key]
            self._store_target_id(entity_type, entity_id, tgt_id)

            done_msg = (
                f"Loaded {entity_type} {entity_id} --> {{"
                f"{self.target_id_key}: {tgt_id}}}"
            )

        # log action
        with count_lock:
            self.counts[entity_type] += 1
            self.logger.info(
                done_msg + f" (#{self.counts[entity_type]} "
                f"of {self.totals[entity_type]})"
            )

    def _validate_run_parameters(self, target_entities):
        """
        Validate the parameters being passed into the _run method. This
        method gets executed before the body of _run is executed.

        `target_entities` should be a dict of lists, keyed by target concepts
        defined in the target_api_config. Each list element must be a dict.
        """
        try:
            assert_safe_type(target_entities, dict)
            assert_all_safe_type(target_entities.keys(), str)
            assert_all_safe_type(target_entities.values(), list)
            for instance_list in target_entities.values():
                assert_all_safe_type(instance_list, dict)

        except TypeError as e:
            raise InvalidIngestStageParameters from e

        invalid_ents = [
            ent
            for ent in target_entities
            if ent not in self.target_api_config.target_concepts
        ]
        if invalid_ents:
            valid_ents = list(self.target_api_config.target_concepts.keys())
            raise InvalidIngestStageParameters(
                f"One or more keys in the _run input dict is invalid: "
                f"{pformat(invalid_ents)}. A key is valid if is one of the "
                f"target concepts: {pformat(valid_ents)} "
                f"specified in {self.target_api_config.config_filepath}"
            )

    def _postrun_concept_discovery(self, run_output):
        pass  # TODO

    def _run(self, target_entities):
        """
        Load Stage internal entry point. Called by IngestStage.run

        :param target_entities: Output data structure from the Transform stage
        :type target_entities: dict
        """
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
        for entity_type in self.target_api_config.target_concepts.keys():
            # Skip entities that are not in user specified list or
            # not in the input data
            if (entity_type not in self.entities_to_load) or (
                entity_type not in target_entities
            ):
                self.logger.info(f"Skipping load of {entity_type}")
                continue

            entities = target_entities[entity_type]

            self.logger.info(f"Begin loading {entity_type}")

            self.prev_entity = None
            self.totals[entity_type] = len(entities)
            self.counts[entity_type] = 0

            if self.use_async:
                ex = concurrent.futures.ThreadPoolExecutor()
                futures = []

            for entity in entities:
                entity_id = entity["id"]
                endpoint = entity["endpoint"]
                body = entity["properties"]
                links = entity["links"]

                if self.use_async and not self.resume_from:
                    futures.append(
                        ex.submit(
                            self._load_entity,
                            entity_type,
                            endpoint,
                            entity_id,
                            body,
                            links,
                        )
                    )
                else:
                    self._load_entity(
                        entity_type, endpoint, entity_id, body, links
                    )

            if self.use_async:
                for f in concurrent.futures.as_completed(futures):
                    f.result()
                ex.shutdown()

            self.logger.info(f"End loading {entity_type}")

        if self.resume_from:
            self.logger.warning(
                f"⚠️ Could not find resume_from target '{self.resume_from}'! "
                "Nothing was actually loaded into the target service."
            )
