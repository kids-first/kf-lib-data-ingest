Developers
===========

Setup Development Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Git clone the repository::

    $ git clone git@github.com:kids-first/kf-lib-data-ingest.git

2. Setup and activate virtual env::

    $ python3 -m venv venv
    $ source venv/bin/activate

3. Install kf-lib-data-ingest along with dependencies::

    $ pip install -e .

4. Install dev requirements::

    $ pip install -r dev-requirements.txt


Run Unit Tests
^^^^^^^^^^^^^^
Use pytest to run tests::

    python -m pytest tests

Build Documentation
^^^^^^^^^^^^^^^^^^^^
We currently use Sphinx for generating the API/reference documentation for the ingest library.

If you would like to develop documentation follow these steps:

1. Install the Python packages needed to build the documentation::

    $ pip install -r doc-requirements.txt

2. Build the static docs site

After making changes to docstrings in the source code or .rst files in docs/source, you will need to rebuild the html::

    $ cd docs
    $ make html

3. This will generate a ``build`` subdirectory containing the generated docs site. View the docs by opening ``docs/build/html/index.html`` in the browser
