import os
from setuptools import setup, find_packages

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, "requirements.txt")
with open(req_file) as f:
    requirements = f.read().splitlines()


setup(
    name="kf-lib-data-ingest",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "version_scheme": "post-release",
    },
    setup_requires=["setuptools_scm"],
    description="Kids First Data Ingest Library",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["kidsfirst=kf_lib_data_ingest.app.cli:cli",],
    },
    package_data={"templates": ["*.tsv"]},
    include_package_data=True,
    install_requires=requirements,
)
