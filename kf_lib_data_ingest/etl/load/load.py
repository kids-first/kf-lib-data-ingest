"""Control shim for differentiating load plugin versions."""
from kf_lib_data_ingest.etl.configuration.target_api_config import (
    TargetAPIConfig,
)


def LoadStage(target_api_config_path, *args, **kwargs):
    target_api_config = TargetAPIConfig(target_api_config_path)
    if (
        hasattr(target_api_config, "LOADER_VERSION")
        and target_api_config.LOADER_VERSION == 2
    ):
        from kf_lib_data_ingest.etl.load.load_v2 import LoadStage
    else:
        from kf_lib_data_ingest.etl.load.load_v1 import LoadStage

    return LoadStage(target_api_config_path, *args, **kwargs)
