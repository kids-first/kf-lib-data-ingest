import os

import pytest
from click.testing import CliRunner

from conftest import KIDS_FIRST_CONFIG, TEST_INGEST_CONFIG
from kf_lib_data_ingest.app import cli
from kf_lib_data_ingest.common.misc import str_to_obj
from kf_lib_data_ingest.common.errors import InvalidIngestStageParameters
from kf_lib_data_ingest.etl.load.load import LoadStage


@pytest.fixture(scope='function')
def load_stage(tmpdir):
    return LoadStage(
        KIDS_FIRST_CONFIG, 'http://URL_A', [], 'FAKE_STUDY_A',
        uid_cache_dir=tmpdir, dry_run=True
    )


@pytest.mark.parametrize('run_input',
                         [
                             ('foo'),
                             ({'foo': 'bar'}),
                             ({'participant': 'foo'}),
                             ({'participant': ['foo']}),
                         ])
def test_invalid_run_parameters(load_stage, caplog, run_input):
    """
    Test running transform with invalid run params
    """
    with pytest.raises(InvalidIngestStageParameters):
        load_stage.run(run_input)


def test_uid_cache(tmpdir):
    a1 = LoadStage(
        KIDS_FIRST_CONFIG, 'http://URL_A', [], 'FAKE_STUDY_A',
        uid_cache_dir=tmpdir, dry_run=True
    )

    a2 = LoadStage(
        KIDS_FIRST_CONFIG, 'http://URL_A', [], 'FAKE_STUDY_A',
        uid_cache_dir=tmpdir, dry_run=True
    )

    assert os.path.exists(a1.uid_cache_filepath)

    a1._store_target_id('entity_type', 'entity_unique_key', 'target_id')
    assert a1._get_target_id('entity_type', 'entity_unique_key') == 'target_id'

    assert os.path.exists(a2.uid_cache_filepath)
    a2._store_target_id('entity_type', 'entity_unique_key', 'target_id')
    assert a2._get_target_id('entity_type', 'entity_unique_key') == 'target_id'

    assert a1.uid_cache_filepath == a2.uid_cache_filepath

    b1 = LoadStage(
        KIDS_FIRST_CONFIG, 'http://URL_B1', [], 'FAKE_STUDY_B',
        uid_cache_dir=tmpdir, dry_run=True
    )

    b2 = LoadStage(
        KIDS_FIRST_CONFIG, 'URL_B2', [], 'FAKE_STUDY_B',
        uid_cache_dir=tmpdir, dry_run=True
    )

    assert 'URL_B2' in b2.uid_cache_filepath
    assert 'URL_B1' in b1.uid_cache_filepath
    assert os.path.exists(b1.uid_cache_filepath)
    assert os.path.exists(b2.uid_cache_filepath)

    b1._store_target_id('entity type', 'entity unique key', 'target_id')
    assert b1._get_target_id('entity type', 'entity unique key') == 'target_id'
    b2._store_target_id('entity type', 'entity_unique_key', 'target id')
    assert b2._get_target_id('entity type', 'entity_unique_key') == 'target id'

    assert b1.uid_cache_filepath != a1.uid_cache_filepath
    assert b1.uid_cache_filepath != b2.uid_cache_filepath


@pytest.mark.parametrize(
    'payload, expected_value',
    [
        ({'age': '10'}, 10),
        ({'affected': 'True'}, True),
        ({'url_list': '["file1.tsv", "file1.tsv", "file1.tsv"]'},
         ["file1.tsv", "file1.tsv", "file1.tsv"]),
        ({'url_set': '{"file1.tsv", "file1.tsv", "file1.tsv"}'},
         {"file1.tsv", "file1.tsv", "file1.tsv"}),
        ({'hash_dict': "{'md5': 'value'}"}, {'md5': 'value'}),
        ({'url_list': 'not a list'}, 'not a list'),
        ({'foo': '10'}, '10'),
        ({'race': '10'}, '10'),
    ]
)
def test_value_transformation(load_stage, payload, expected_value):
    schema = {
        'age': ('PARTICIPANT.AGE', int),
        'affected': ('PARTICIPANT.STATUS', bool),
        'volume': ('SPECIMEN.VOLUME', float),
        'url_list': ('GENOMIC_FILE.URL_LIST', str_to_obj),
        'url_set': ('GENOMIC_FILE.URL_SET', str_to_obj),
        'hash_dict': ('GENOMIC_FILE.HASH_DICT', str_to_obj),
        'property': None,
        'race': 'PARTICIPANT.RACE'
    }
    payload = load_stage._apply_property_value_transformations(schema, payload)
    val = list(payload.values())[0]
    assert val == expected_value


def test_ingest_load_async_error():
    """
    Test that async loading exits when threads raise exceptions
    """
    prev_environ = os.environ.get('MAX_RETRIES_ON_CONN_ERROR')
    os.environ['MAX_RETRIES_ON_CONN_ERROR'] = '0'

    runner = CliRunner()
    result = runner.invoke(
        cli.ingest,
        [TEST_INGEST_CONFIG, '--use_async', '--target_url', 'http://potato']
    )
    assert result.exit_code == 1

    if prev_environ:
        os.environ['MAX_RETRIES_ON_CONN_ERROR'] = prev_environ
    else:
        del os.environ['MAX_RETRIES_ON_CONN_ERROR']
