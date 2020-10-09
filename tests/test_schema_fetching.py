import logging
import os

import requests_mock

from conftest import KIDSFIRST_DATASERVICE_PROD_URL, TEST_DATA_DIR
from kf_lib_data_ingest.common.io import read_json
from kf_lib_data_ingest.network.utils import get_open_api_v2_schema

schema_url = f"{KIDSFIRST_DATASERVICE_PROD_URL}/swagger"
mock_dataservice_schema = read_json(
    os.path.join(TEST_DATA_DIR, "mock_dataservice_schema.json")
)

logger = logging.getLogger("test_logger")


@requests_mock.Mocker(kw="mock")
def test_get_kf_schema_server_down(caplog, tmpdir, **kwargs):
    """
    Test kf_lib_data_ingest.target_api_plugins.get_open_api_v2_schema

    Test retrieval when server is down and no cached schema exists yet
    """
    url = "http://localhost:1234"
    mock = kwargs["mock"]
    mock.get(f"{url}/swagger", status_code=500)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test retrieval when server is down and no cached schema exists
    cached_schema_file = os.path.join(tmpdir, "cached_schema.json")
    output = get_open_api_v2_schema(
        url, cached_schema_filepath=cached_schema_file, logger=logger
    )
    assert output is None
    assert "Unable to retrieve target schema" in caplog.text
    assert not os.path.isfile(cached_schema_file)


@requests_mock.Mocker(kw="mock")
def test_get_kf_schema(caplog, tmpdir, target_api_config, **kwargs):
    """
    Test kf_lib_data_ingest.network.utils.get_open_api_v2_schema

    Test retrieval when server is up and no cached schema exists
    Test retrieval when servier is down and cached schema exists
    """
    # Set up mock responses
    mock = kwargs["mock"]
    mock.get(schema_url, json=mock_dataservice_schema)

    # Set pytest to capture log events at level INFO or higher
    caplog.set_level(logging.INFO)

    # Test server up, no cache file exists
    cached_schema_file = os.path.join(tmpdir, "cached_schema.json")
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        cached_schema_filepath=cached_schema_file,
    )
    assert output.get("definitions")
    assert output.get("version")
    assert output.get("target_service")
    assert os.path.isfile(cached_schema_file)

    # Test retrieval when server is down and cached schema exists
    mock.get(schema_url, status_code=500)
    output = get_open_api_v2_schema(
        KIDSFIRST_DATASERVICE_PROD_URL,
        cached_schema_filepath=cached_schema_file,
    )
    assert output.get("definitions")
    assert output.get("version")
    assert output.get("target_service")

    # Test cached schema file created in default loc
    mock.get(schema_url, json=mock_dataservice_schema)
    output = get_open_api_v2_schema(KIDSFIRST_DATASERVICE_PROD_URL)
    assert os.path.isfile(os.path.realpath("./cached_schema.json"))
