========================
Debugging and Developing
========================

Run Subsets of the Pipeline
===========================

As you develop your ingest package, you will likely run into scenarios where
you do not want to run the entire ingest pipeline.

The CLI allows you to run a subset of the ingest stages to make development
easier. The ``--stages`` option takes any combination of the ingest stage
codes, ``e, t, l``.

For example:

.. code-block:: text

  $ kidsfirst test my_study --stages=e  # will only run the extract stage
  $ kidsfirst test my_study --stages=et # will run extract then transform
  $ kidsfirst test my_study --stages=te # will run extract then transform
  $ kidsfirst test my_study --stages=l  # will only run the load stage

There are a few important things to note here:

- Order of the stage codes does not matter because the ingest pipeline will
  always execute the stages in the right order (extract, transform, load).
- When running an ingest stage via the ``--stages`` option, the output from
  the previous stage must exist, otherwise an error will occur.

Stage Outputs
=============

Every ingest stage has the option to write its output to a directory that
follows this path pattern:
``<ingest package dir>/output/<name of stage>``

Ingest Log
==========

The ingest pipeline logs messages to the console and also writes the same
messages to a log file. By default the log file is stored at:
``<ingest package dir>/logs/ingest.log``

The output directory for logs can be changed by setting the ``log_dir``
attribute in the ingest package config file.

The name of the log file can be changed to include the ingest execution
timestamp by setting the ``overwrite_log`` attribute to False. The default
is ``overwrite_log=True`` which causes the log to be written to a file
called ``ingest.log`` that gets overwritten each time the ingest pipeline
runs. If ``overwrite_log=False``, then a new log file will be written each time
the ingest pipeline runs, and it will follow this naming pattern:
``ingest_<ISO 8601 formatted timestamp>.log``
