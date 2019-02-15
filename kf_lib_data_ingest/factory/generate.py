"""
Module to keep code for generating code or files
"""

import os
import shutil

from kf_lib_data_ingest.config import INGEST_PKG_TEMPLATE_NAME


def new_ingest_pkg(dest_dir=None):
    """
    Create a new ingest package using the template defined in
    ./templates/study

    :param dest_dir: (Optional) path to the dir where ingest package will be
    created
    :type dest_dir: str
    :return: dest_dir
    """
    # Make destination dir path
    if not dest_dir:
        dest_dir = os.getcwd()
        dest_dir = os.path.join(dest_dir, INGEST_PKG_TEMPLATE_NAME)
    else:
        # Expand . and ~
        dest_dir = os.path.expanduser(dest_dir)
        dest_dir = os.path.realpath(dest_dir)

    # Don't overwrite existing ingest dirs
    if os.path.isdir(dest_dir):
        raise FileExistsError(
            f'Cannot create a new ingest package here: {dest_dir}. '
            'Directory already exists!')

    # Create package
    src_dir = os.path.join(os.path.dirname(__file__), 'templates',
                           INGEST_PKG_TEMPLATE_NAME)
    shutil.copytree(src_dir, dest_dir)

    return dest_dir
