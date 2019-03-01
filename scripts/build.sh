#!/bin/bash

# Abort the script if there is a non-zero error
set -e

python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip
python setup.py install_egg_info
pip install -e .

# Run ingest to ensure the build worked
# Only output exceptions so we know what went wrong
kidsfirst ingest tests/data/test_study --log_level=error

pip install -r doc-requirements.txt

cd docs
doc8 source
make html
cd -
