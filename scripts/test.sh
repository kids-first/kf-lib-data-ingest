#!/bin/bash

# Abort the script if there is a non-zero error
set -e

. venv/bin/activate
pip install -r dev-requirements.txt
py.test --cov=kf_lib_data_ingest tests
py.test --codestyle kf_lib_data_ingest
