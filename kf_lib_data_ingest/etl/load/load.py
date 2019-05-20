"""
Module for loading the transform output into the dataservice.
"""
import logging
import os
from collections import defaultdict
from pprint import pformat
from urllib.parse import urlparse

import pandas
import requests
from requests import RequestException
from sqlite3worker import Sqlite3Worker, sqlite3worker

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import multisplit
from kf_lib_data_ingest.common.stage import IngestStage
from kf_lib_data_ingest.network.utils import requests_retry_session
from kf_lib_data_ingest.config import DEFAULT_ID_CACHE_FILENAME
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig
)

sqlite3worker.LOGGER.setLevel(logging.WARNING)


def send(req):
    return requests_retry_session().send(req)


class LoadStage(IngestStage):
    def __init__(
        self, target_api_config_path, target_url, entities_to_load,
        study_id, output_dir, uid_cache_dir=None, use_async=False,
        dry_run=False
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
        :param output_dir: where to store output
        :type output_dir: str
        :param uid_cache_dir: where to find the ID cache, defaults to None
        :type uid_cache_dir: str, optional
        :param use_async: use asynchronous networking, defaults to False
        :type use_async: bool, optional
        :param dry_run: don't actually transmit, defaults to False
        :type dry_run: bool, optional
        """
        super().__init__(ingest_output_dir=output_dir)
        self.target_api_config = TargetAPIConfig(target_api_config_path)
        self.concept_targets = {
            v['standard_concept'].UNIQUE_KEY: k
            for k, v in self.target_api_config.target_concepts.items()
        }
        self.target_id_key = (
            self.target_api_config.contents.target_service_entity_id
        )

        self.entities_to_load = entities_to_load
        self.target_url = target_url
        self.dry_run = dry_run
        self.study_id = study_id

        target = urlparse(target_url).netloc or urlparse(target_url).path
        self.uid_cache_filepath = os.path.join(
            uid_cache_dir or os.getcwd(),
            #  Every target gets its own cache because they don't share UIDs
            '_'.join(multisplit(target, [':', '/']))
            #  Every study gets its own cache to compartmentalize internal IDs
            + '_' + study_id
            + '_' + DEFAULT_ID_CACHE_FILENAME
        )

        if not os.path.isfile(self.uid_cache_filepath):
            self.logger.info(
                'Target UID cache file not found so a new one will be created:'
                f' {self.uid_cache_filepath}'
            )

        # Two-stage (RAM + disk) cache
        self.uid_cache = defaultdict(dict)
        self.uid_cache_db = Sqlite3Worker(self.uid_cache_filepath)

        self.entity_cache = dict()

        self.use_async = use_async

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
                ' (unique_id TEXT PRIMARY KEY, target_id TEXT);'
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
                ' (unique_id, target_id)'
                ' VALUES (?,?);', (entity_id, target_id)
            )

    def _read_output(self):
        pass  # TODO

    def _write_output(self, output):
        pass  # TODO

    def _prepare_patch(self, endpoint, target_id, body):
        """
        Prepare a PATCH request for the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param target_id: which target service ID to patch
        :type target_id: str
        :param body: map between entity keys and values
        :type body: dict
        :return: prepared PATCH request object
        :rtype: requests.PreparedRequest
        """
        host = self.target_url
        return requests.Request(
            method='PATCH',
            url='/'.join([v.strip('/') for v in [host, endpoint, target_id]]),
            json=body
        ).prepare()

    def _prepare_post(self, endpoint, body):
        """
        Prepare a POST request for the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param body: map between entity keys and values
        :type body: dict
        :return: prepared POST request object
        :rtype: requests.PreparedRequest
        """
        host = self.target_url
        return requests.Request(
            method='POST',
            url='/'.join([v.strip('/') for v in [host, endpoint]]),
            json=body
        ).prepare()

    def _prepare_get(self, endpoint, body):
        """
        Prepare a GET request for the target service.

        :param endpoint: which target service endpoint to hit
        :type endpoint: str
        :param body: filter arguments keys and values
        :type body: dict
        :return: prepared GET request object
        :rtype: requests.PreparedRequest
        """
        host = self.target_url
        return requests.Request(
            method='GET',
            url='/'.join([v.strip('/') for v in [host, endpoint]]),
            params=body
        ).prepare()

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
            self.logger.debug(
                f'Trying to PATCH {entity_type} {target_id}.'
            )
            req = self._prepare_patch(
                endpoint, target_id, body
            )
            resp = send(req)
            if resp.status_code == 404:
                self.logger.debug(
                    f'Entity {entity_type} {target_id} not found in target '
                    'service.'
                )
                resp = None

        if not resp:
            self.logger.debug(
                f'Trying to POST new {entity_type} {entity_id}.'
            )
            req = self._prepare_post(endpoint, body)
            resp = send(req)

        self.logger.debug(f'Request body: \n{pformat(body)}')

        if resp.status_code in {200, 201}:
            result = resp.json()['results']
            self.logger.debug(f'Response body:\n{pformat(result)}')
            return result
        elif (
            (resp.status_code == 400)
            and ("already exists" in resp.json()['_status']['message'])
        ):
            # Our dataservice returns 400 if a relationship already exists
            # even though that's a silly thing to do.
            # See https://github.com/kids-first/kf-api-dataservice/issues/419
            req = self._prepare_get(endpoint, body)
            result = send(req).json()['results'][0]
            self.logger.debug(f'Already exists:\n{pformat(result)}')
            return result
        else:
            self.logger.debug(f'Response error:\n{pformat(resp.__dict__)}')
            raise RequestException(resp.text)

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

        # populate target uid
        body[self.target_id_key] = (
            body.get(self.target_id_key)
            or self._get_target_id(entity_type, entity_id)
        )

        # don't send null attributes
        body = {k: v for k, v in body.items() if not pandas.isnull(v)}

        # link cached foreign keys
        target_concepts = self.target_api_config.target_concepts
        for link_key, link_value in links.items():
            if link_key == 'study_id':
                body[link_key] = self.study_id
            else:
                link_concept_key = (
                    target_concepts[entity_type]['links'][link_key]
                )
                link_type = self.concept_targets[link_concept_key]
                body[link_key] = self._get_target_id(link_type, link_value)

        if self.dry_run:
            # Fake sending with fake foreign keys
            for link_key, link_value in links.items():
                body[link_key] = f'DRY_{link_value}'

            instance_id = body.get(self.target_id_key)
            if instance_id:
                req_method = 'PATCH'
                id_str = f' {{{self.target_id_key}: {instance_id}}}'
            else:
                req_method = 'POST'
                id_str = ''

            self.logger.info(f'DRY RUN - {req_method} {endpoint}{id_str}')
            self.logger.debug(f'Request body preview:\n{pformat(body)}')
        else:
            # send to the target service
            entity = self._submit(entity_id, entity_type, endpoint, body)

            # cache source_ID:target_ID lookup
            tgt_id = entity[self.target_id_key]
            self._store_target_id(entity_type, entity_id, tgt_id)

            # log action
            self.logger.info(
                f'Loaded {entity_type} {entity_id} --> {{'
                f'{self.target_id_key}: {tgt_id}}}'
            )

    def _validate_run_parameters(self, target_entities):
        pass  # TODO

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
                'DRY RUN mode is ON. No entities will be loaded into the '
                'target service.'
            )
        for entity_type, entities in target_entities.items():
            if entity_type not in self.entities_to_load:
                self.logger.info(f'Skipping load of {entity_type}')
                continue

            self.logger.info(f'Begin loading {entity_type}')

            self.prev_entity = None
            total = len(entities)
            for i, entity in enumerate(entities):
                entity_id = entity['id']
                endpoint = entity['endpoint']
                body = entity['properties']
                links = entity['links']

                self.logger.info(
                    f'Loading {entity_type} {entity_id} (#{i+1} of {total})'
                )
                self._load_entity(
                    entity_type, endpoint, entity_id, body, links
                )

            self.logger.info(f'End loading {entity_type}')
