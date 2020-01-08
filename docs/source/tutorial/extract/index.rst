.. _Tutorial-Extract-Stage:

=============
Extract Stage
=============

The extract stage does 3 main things:

1. Select or extract the desired subset of data from the source data files
2. Clean the selected data (remove trailing whitespaces, etc)
3. Map the cleaned data's attributes and values to Kids First entity attributes
   and acceptable values.

The extract configuration files instruct the extract stage how to accomplish
the above.

.. include:: extract_example.inc

.. include:: map_preprocessing.inc

.. include:: map_custom_reader.inc

.. include:: map_operations.inc

.. include:: map_operation_strategic_details.inc
