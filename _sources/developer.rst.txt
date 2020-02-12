**********
Developers
**********

Set Up Development Environment
==============================

1. Git clone the repository::

    $ git clone git@github.com:kids-first/kf-lib-data-ingest.git

2. Create and activate virtual environment::

    $ python3 -m venv venv
    $ source venv/bin/activate

3. Install kf-lib-data-ingest along with dependencies::

    $ pip install -e .

4. Install dev requirements::

    $ pip install -r dev-requirements.txt


Run Unit Tests
==============

Use pytest to run tests::

    pytest tests

Build Documentation
===================

We use Sphinx to generate the reference documentation. If you would like to
develop documentation, follow these steps:

1. Install the Python packages needed to build the documentation::

    $ pip install -r doc-requirements.txt

2. Build the static docs site::

    $ cd docs
    $ make html

3. View the docs by opening ``docs/build/html/index.html`` in a browser.
