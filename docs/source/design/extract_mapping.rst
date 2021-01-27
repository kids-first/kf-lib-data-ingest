.. _Extract-Mapping:

===============
Extract Mapping
===============

The extract process follows the following basic pattern:

1. Read file data into a table with headers.
2. Preprocess the table.
3. Look at some subset of data from the table.
4. Apply some manipulations to the subset.
5. Write the result to one or more output columns adhering to the principle of
   :ref:`composing a series of simple and unambiguous factual statements
   <Design-Overview>`.
6. If not done, go to #3

Each iteration of steps 3-5 is called a mapping operation.

The goal is to map the investigator's columns to our columns and convert their
values to our values.
