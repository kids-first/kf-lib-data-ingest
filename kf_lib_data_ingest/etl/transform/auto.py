"""
Module for 'auto' transforming source data into target service entities.
Auto transformation does not require guidance from the user on how to merge
source data tables.
"""
from kf_lib_data_ingest.etl.transform.standard_model.model import StandardModel
from kf_lib_data_ingest.etl.transform.transform import TransformStage


class AutoTransformStage(TransformStage):
    def _do_transform(self, data_dict):
        """
        See TransformStage._do_transform
        """
        self.logger.info('Begin auto transformation ...')

        # Insert mapped dataframes into the standard model
        model = StandardModel(logger=self.logger)
        model.populate(data_dict)

        # Transform the concept graph into target entities
        target_entities = model.transform(self.target_api_config)

        return target_entities
