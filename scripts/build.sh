#!/bin/bash

# Abort the script if there is a non-zero error
set -e

python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip
python setup.py install_egg_info
pip install -e .
pip install -r doc-requirements.txt

doc8 docs/source
make -C docs html
