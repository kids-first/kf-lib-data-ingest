"""
For Version 1 Target Service Plugins
"""
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.etl.load.load_base import LoadStageBase


class LoadStage(LoadStageBase):
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
            return self._get_target_id_from_key(
                entity_class.class_name, entity_class.build_key(record)
            )
        except AssertionError:
            return None

    def _do_target_submit(self, entity_class, body):
        """Shim for target API submission across loader versions"""
        return self.target_api_config.submit(
            self.target_url, entity_class, body
        )

    def _do_target_get_key(self, entity_class, record):
        """Shim for target API key building across loader versions"""
        return entity_class.build_key(record)

    def _do_target_get_entity(self, entity_class, record, keystring):
        """Shim for target API entity building across loader versions"""
        return entity_class.build_entity(
            record, keystring, self._get_target_id_from_record
        )
