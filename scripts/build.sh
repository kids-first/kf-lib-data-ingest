#!/bin/bash

# Abort the script if there is a non-zero error
set -e

python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip
pip install -e .
pip install -r dev-requirements.txt
pip install -r doc-requirements.txt

cd docs
make html
cd -
