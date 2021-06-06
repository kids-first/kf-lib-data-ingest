"""Control shim for differentiating load plugin versions."""
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)
from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
)


def LoadStage(target_api_config_path, *args, **kwargs):
    target_api_config = TargetAPIConfig(target_api_config_path)
    if (not hasattr(target_api_config, "LOADER_VERSION")) or (
        target_api_config.LOADER_VERSION == 1
    ):
        ver = 1
        from kf_lib_data_ingest.etl.load.load_v1 import LoadStage
    elif target_api_config.LOADER_VERSION == 2:
        ver = 2
        from kf_lib_data_ingest.etl.load.load_v2 import LoadStage
    else:
        raise ConfigValidationError(
            f"Target API plugin has invalid LOADER_VERSION: {target_api_config.LOADER_VERSION}\n"
            "Must be one of: [1, 2]"
        )

    loader = LoadStage(target_api_config_path, *args, **kwargs)
    loader.logger.info(f"Loader Version: {ver}")
    return loader
