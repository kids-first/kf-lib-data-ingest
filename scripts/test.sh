#!/bin/bash

# Abort the script if there is a non-zero error
set -e

. venv/bin/activate
py.test --cov=kf_lib_data_ingest tests
py.test --codestyle kf_lib_data_ingest
