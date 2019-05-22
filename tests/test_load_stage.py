import os

import pytest

from conftest import KIDS_FIRST_CONFIG
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
