===========================================
Sharing Operations Across Multiple Extracts
===========================================

Scenario
========

You have ten data files, and you want to do the same set of extraction
operations in all of them.

Solution with Imports
=====================

In this example solution we would make a shared_operations.py file containing
whatever set of operations we want to share, and we'll put it somewhere outside
of our designated extract_configs directory (so that it isn't treated as an
extract config) like this::

    my_study/
    ├── ingest_package_config.py
    ├── extract_configs/
    │   ├── my_extract_config.py
    │   └── ...
    ├── shared_operations.py     <- contains shared operations
    └── transform_module.py

Then in the extract configuration files, import the shared operations like this
(using the library method for turning a file into an importable module):

.. code-block:: Python

    from pathlib import Path
    from kf_lib_data_ingest.common.misc import import_module_from_file

    shmod = import_module_from_file(
        Path(Path(__file__).parent.parent, 'shared_operations.py')
    )
    operations = shmod.operations + [
        # new operations go here
    ]

or like this (using the standard Python importer):

.. code-block:: Python

    from kf_ingest_packages.packages.my_study.shared_operations import operations as shared

    operations = shared + [
        # new operations go here
    ]

.. caution:: If you use the standard Python importer, be careful to not modify
  the imported shared operations list, because it will change everywhere.
