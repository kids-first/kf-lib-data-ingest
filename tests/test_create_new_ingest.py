import os
import shutil
import filecmp

import pytest

from kf_lib_data_ingest.factory.generate import new_ingest_pkg
from kf_lib_data_ingest.config import (
    INGEST_PKG_TEMPLATE_NAME,
    ROOT_DIR
)


@pytest.mark.parametrize('dest_dir, expected_dir', [
    ('./study_1', os.path.join(os.path.dirname(ROOT_DIR), 'study_1')),
    ('study_1', os.path.join(os.path.dirname(ROOT_DIR), 'study_1')),
    (None, os.path.join(os.path.dirname(ROOT_DIR), 'my_study'))
])
def test_new_ingest_pkg(dest_dir, expected_dir):
    """
    Test create new ingest package
    """
    # Clean up anything that might not have been cleaned if test failed
    if os.path.isdir(expected_dir):
        shutil.rmtree(expected_dir)

    ret_dest_dir = new_ingest_pkg(dest_dir)

    # Created dest dir = expected path
    assert ret_dest_dir == expected_dir

    # Created dest dir exists
    assert os.path.isdir(ret_dest_dir)

    # Compare template dir with created dir
    template_dir = os.path.join(ROOT_DIR, 'factory', 'templates',
                                INGEST_PKG_TEMPLATE_NAME)
    cmp_out = filecmp.dircmp(template_dir, ret_dest_dir)
    diff = set(cmp_out.left_list).symmetric_difference(set(cmp_out.right_list))
    assert len(diff) == 0

    # Clean up after test
    if os.path.isdir(ret_dest_dir):
        shutil.rmtree(ret_dest_dir)


def test_new_ingest_already_exists(tmpdir):
    """
    Test attempt to create a new ingest pkg when one with same path already
    exists
    """
    dest_dir = os.path.join(tmpdir, 'new_study')
    ret_dest_dir = new_ingest_pkg(dest_dir)

    with pytest.raises(FileExistsError) as e:
        try:
            new_ingest_pkg(ret_dest_dir)
        except FileExistsError as e:
            assert 'Cannot create a new ingest package' in str(e)
            assert 'Directory already exists' in str(e)
            raise
