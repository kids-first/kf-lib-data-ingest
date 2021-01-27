************
Installation
************

Install ``psycopg2``. For the purposes of using this library, you might be able
to safely run::

    $ pip install psycopg2-binary

.. note::
    If you plan to ingest without :ref:`remote warehousing<Warehousing>`, then you won't need psycopg2.

    See: :ref:`DisablingWarehousing`.

If a precompiled binary for psycopg2 doesn't exist for your platform, or
if you prefer not to use one, installation with pip may not be straightforward
everywhere (see e.g.
https://web.archive.org/web/20190908200859/https://stackoverflow.com/questions/26288042/error-installing-psycopg2-library-not-found-for-lssl),
so we leave it as an exercise for the reader.

Install the latest release of ``kf-lib-data-ingest`` with ``pip``::

    $ pip install git+https://github.com/kids-first/kf-lib-data-ingest.git@latest-release

Or install the latest development changes::

    $ pip install git+https://github.com/kids-first/kf-lib-data-ingest.git

Now you can run ``kidsfirst -h`` to see all subcommands and descriptions or run
``kidsfirst <command> -h`` to see help for subcommands.
