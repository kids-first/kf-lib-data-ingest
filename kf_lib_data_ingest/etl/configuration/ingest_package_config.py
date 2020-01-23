import logging
import os

from kf_lib_data_ingest.common.type_safety import (
    assert_all_safe_type,
    assert_safe_type,
)
from kf_lib_data_ingest.config import (
    INGEST_PACKAGE_CONFIG_DEFAULT_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_OVERWRITE_OPT,
)
from kf_lib_data_ingest.etl.configuration.base_config import PyModuleConfig


class IngestPackageConfig(PyModuleConfig):
    # TODO
    # Removal/Exclusion list
    # We periodically get messages from investigators saying that we should
    # remove/exclude certain items from the dataservice.
    # ( e.g. https://github.com/kids-first/kf-study-imports/
    # issues/75#issuecomment-405245066 )
    # We should be able to store a list of either external IDs and
    # their types or KFIDs of things to delete from the dataservice if already
    # there and to prevent from loading subsequently.

    def __init__(self, config_path):
        """
        Construct a IngestPackageConfig object from a config file

        :param config_path: Path to the data ingest config file
        """
        # If given config path is a dir, create default config file path
        config_path = os.path.abspath(os.path.expanduser(config_path))
        if os.path.isdir(config_path):
            config_path = os.path.join(
                config_path, INGEST_PACKAGE_CONFIG_DEFAULT_FILENAME
            )

        super().__init__(config_path)

        # Directory containing the extract config files
        self.extract_config_dir = os.path.join(
            os.path.dirname(self.config_filepath), self.extract_config_dir
        )

        self._set_log_params()

    def _set_log_params(self):
        """
        Set log parameters
        """

        def _default_log_dir():
            """
            Create default log directory
            """
            config_dir = os.path.abspath(os.path.dirname(self.config_filepath))
            log_dir = os.path.join(config_dir, "logs")
            if not os.path.isdir(log_dir):
                os.mkdir(log_dir)
            return log_dir

        # Set default log values
        log_params = {
            "log_dir": _default_log_dir,  # don't call yet unless we need it!
            "log_level": DEFAULT_LOG_LEVEL,
            "overwrite_log": DEFAULT_LOG_OVERWRITE_OPT,
        }
        for param, default_value in log_params.items():
            if not hasattr(self.contents, param):
                if callable(default_value):
                    setattr(self.contents, param, default_value())
                else:
                    setattr(self.contents, param, default_value)

        # Convert log_level string to numeric value
        if isinstance(self.contents.log_level, str):
            self.contents.log_level = logging._nameToLevel.get(
                self.contents.log_level.upper(), DEFAULT_LOG_LEVEL
            )

    def _validate(self):
        assert self.extract_config_dir is not None
        assert_safe_type(self.extract_config_dir, str)

        assert self.study is not None
        assert_safe_type(self.study, str)

        assert self.target_service_entities is not None
        assert_safe_type(self.target_service_entities, list)
        assert_all_safe_type(self.target_service_entities, str)
