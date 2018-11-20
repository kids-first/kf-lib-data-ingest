#!/bin/bash

. venv/bin/activate
py.test --cov=kf_lib_data_ingest tests
py.test --pep8 kf_lib_data_ingest
