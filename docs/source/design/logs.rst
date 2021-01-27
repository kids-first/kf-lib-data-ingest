====
Logs
====

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
``ingest_<ISO 8601 timestamp>.log``

Stage Outputs
=============

Ingest stages also write their own output to a directory that follows this path
pattern: ``<ingest package dir>/output/<stage>``
