#!/bin/bash

python3 -m venv venv
. venv/bin/activate
pip install -e .
pip install -r dev-requirements.txt
pip install -r doc-requirements.txt
