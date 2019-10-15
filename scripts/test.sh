#!/bin/bash

# Abort the script if there is a non-zero error
set -e

. venv/bin/activate
pip install -r dev-requirements.txt
py.test --cov=kf_lib_data_ingest tests

pycodestyle --ignore=E501,W503,E203 kf_lib_data_ingest tests
black --check --line-length 79 kf_lib_data_ingest tests
