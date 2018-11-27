#!/bin/bash

. venv/bin/activate
py.test --cov=kf_lib_data_ingest tests
py.test --codestyle kf_lib_data_ingest
