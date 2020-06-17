#!/bin/bash

# Abort the script if there is a non-zero error
set -e

. venv/bin/activate
pip install -r dev-requirements.txt
py.test -s --cov=kf_lib_data_ingest tests

black --check --line-length 80 kf_lib_data_ingest tests
flake8 --ignore=E501,W503,E203 kf_lib_data_ingest
flake8 --ignore=E501,W503,E203 tests
