.. _Extract-Mapping:

===============
Extract Mapping
===============

The extract process follows the following basic pattern:

1. Read file data into a table with headers.
2. Preprocess into new table.
3. Look at some subset of data from that table.
4. Apply some transformation to the subset.
5. Write the result to one or more output columns adhering to the principle of
   :ref:`composing a series of simple and unambiguous factual statements
   <Design-Overview>`.
6. If not done, go to #3

Each iteration of steps 3-5 is called a mapping operation.
