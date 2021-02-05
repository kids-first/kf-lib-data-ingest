"""
Version 2
Module for loading the transform output into the dataservice. It converts the
merged source data into complete message payloads according to a given API
specification, and then sends those messages to the target server.
"""
from pprint import pformat

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.load.load_v1 import LoadStage as LoadBase


class LoadStage(LoadBase):
    def __init__(self, *args, query_url="", **kwargs):
        """
        :param query_url: Alternative API query URL instead of asking the load target
        :type query_url: str, optional
        """
        super().__init__(*args, **kwargs)
        self.query_url = query_url

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
        # check if target ID is given
        tic = record.get(entity_class.target_id_concept)
        if tic and (tic != constants.COMMON.NOT_REPORTED):
            return tic

        # check the cache
        try:
            key_components = self._do_target_get_key(entity_class, record)
            tic = self._get_target_id_from_key(
                entity_class.class_name, str(key_components)
            )
        except Exception:
            return None

        if tic:
            return tic

        # check the server
        if self.dry_run and not self.query_url:
            return None

        err = False
        try:
            tic_list = entity_class.query_target_ids(
                self.query_url or self.target_url, key_components
            )
            if tic_list:
                if len(tic_list) > 1:
                    err = True
                    raise Exception(
                        "Ambiguous query. Multiple target identifiers found.\n"
                        "Sent:\n"
                        f"{pformat(key_components)}\n"
                        "Found:\n"
                        f"{tic_list}"
                    )
                tic = tic_list[0]
                if tic and (tic != constants.COMMON.NOT_REPORTED):
                    self._store_target_id_for_key(
                        entity_class.class_name,
                        str(key_components),
                        tic,
                        self.dry_run,
                    )
                    return tic
        except Exception:
            if err:
                raise

        return None

    def _do_target_submit(self, entity_class, body):
        """Shim for target API submission across loader versions"""
        return entity_class.submit(self.target_url, body)

    def _do_target_get_key(self, entity_class, record):
        """Shim for target API key building across loader versions"""
        return entity_class.get_key_components(
            record, self._get_target_id_from_record
        )

    def _do_target_get_entity(self, entity_class, record, keystring):
        """Shim for target API entity building across loader versions"""
        return entity_class.build_entity(
            record, self._get_target_id_from_record
        )
