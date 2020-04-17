************
Installation
************

Install ``psycopg2``. This can't be straightforwardly done with pip on all platforms (see e.g. https://web.archive.org/web/20190908200859/https://stackoverflow.com/questions/26288042/error-installing-psycopg2-library-not-found-for-lssl), so we leave it as an exercise for the reader.

Install the current release of ``kf-lib-data-ingest`` with ``pip``::

    $ pip install -e git+https://github.com/kids-first/kf-lib-data-ingest.git#egg=kf-lib-data-ingest

Run the CLI to use the library::

    $ kidsfirst ingest /path/to/ingest_package_config.py

Run ``kidsfirst -h`` to see all subcommands and descriptions or run
``kidsfirst <command> -h`` to see help for subcommands.
