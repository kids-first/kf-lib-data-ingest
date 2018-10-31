#!/bin/bash

python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
pip install -r doc-requirements.txt
pip install -e .
