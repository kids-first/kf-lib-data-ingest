#!/bin/bash

. venv/bin/activate
py.test --cov=src tests
py.test --pep8 src
