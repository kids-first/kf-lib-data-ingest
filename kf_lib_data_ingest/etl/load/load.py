"""
Module for loading the transform output into the dataservice.
"""
import logging
import os
from abc import abstractmethod
from collections import defaultdict
from urllib.parse import urlparse

import pandas
import requests
from sqlite3worker import Sqlite3Worker, sqlite3worker

from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.common.misc import multisplit, requests_retry_session
from kf_lib_data_ingest.common.stage import IngestStage
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
        study_id, uid_cache_dir=None, use_async=False, dry_run=False
    ):
        super().__init__()
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
        if entity_type not in self.uid_cache:
            # Create table in DB first if necessary
            self.uid_cache_db.execute(
                f'CREATE TABLE IF NOT EXISTS {entity_type}'
                ' (unique_key TEXT PRIMARY KEY, target_id TEXT);'
            )
            # Populate RAM cache from DB
            for unique_key, target_id in self.uid_cache_db.execute(
                f'SELECT unique_key, target_id FROM \'{entity_type}\';'
            ):
                self.uid_cache[entity_type][unique_key] = target_id

    def _get_target_id(self, entity_type, entity_unique_key):
        self._prime_uid_cache(entity_type)
        return self.uid_cache[entity_type].get(entity_unique_key)

    def _store_target_id(self, entity_type, entity_unique_key, target_id):
        self._prime_uid_cache(entity_type)
        if self.uid_cache[entity_type].get(entity_unique_key) != target_id:
            self.uid_cache[entity_type][entity_unique_key] = target_id
            self.uid_cache_db.execute(
                f'INSERT OR REPLACE INTO {entity_type} (unique_key, target_id)'
                'VALUES (?,?);', (entity_unique_key, target_id)
            )

    def _read_output(self):
        pass  # TODO

    def _write_output(self, output):
        pass  # TODO

    def _prepare_patch(self, host, what, body):
        target_id = body[self.target_id_key]
        return requests.Request(
            method='PATCH',
            url='/'.join([v.strip('/') for v in [host, what, target_id]]),
            json=body
        ).prepare()

    def _prepare_post(self, host, what, body):
        return requests.Request(
            method='POST',
            url='/'.join([v.strip('/') for v in [host, what]]),
            json=body
        ).prepare()

    def _submit(self, entity_type, endpoint, body):
        if self.target_id_key in body:
            req = self._prepare_patch(self.target_url, endpoint, body)
            resp = send(req)
            if resp.status_code == 404:
                req = self._prepare_post(self.target_url, endpoint, body)
                resp = send(req)
        else:
            req = self._prepare_post(self.target_url, endpoint, body)
            resp = send(req)

        self.logger.info(f'Sent {req.method} {req.body}')

        if resp.status_code in {200, 201}:
            return resp.json()['results']
        elif not (
            # Our dataservice returns 400 if a relationship already exists
            # even though that's a silly thing to do.
            # See https://github.com/kids-first/kf-api-dataservice/issues/419
            (resp.status_code == 400)
            and ("already exists" in resp.json()['_status']['message'])
        ):
            raise Exception(resp.__dict__)

    def _load_entity(self, entity_type, entity):
        endpoint = entity['endpoint']
        body = entity['properties']

        # populate target uid
        body[self.target_id_key] = (
            body.get(self.target_id_key)
            or self._get_target_id(entity_type, entity['id'])
        )

        # don't send null attributes
        body = {k: v for k, v in body.items() if not pandas.isnull(v)}

        # link cached foreign keys
        target_concepts = self.target_api_config.target_concepts
        for link_key, link_value in entity['links'].items():
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
            for link_key, link_value in entity['links'].items():
                body[link_key] = f'DRY_{link_value}'
            self.logger.info(f'DRY {endpoint} {body}')
        else:
            # send to the target service
            instance = self._submit(entity_type, endpoint, body)

            # cache result
            tgt_id = instance[self.target_id_key]
            self._store_target_id(entity_type, entity['id'], tgt_id)
            self.logger.info(
                f'Got {entity_type} {entity["id"]} -> {tgt_id} - {instance}'
            )

    def _validate_run_parameters(self, target_entities):
        pass  # TODO

    def _postrun_concept_discovery(self, run_output):
        pass  # TODO

    def _run(self, target_entities):
        for entity_type, entities in target_entities.items():
            self.prev_entity = None
            for entity in entities:
                self._load_entity(entity_type, entity)
