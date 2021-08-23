.. _Map-Operations:

Predefined Mapping Operations
=============================

.. note::
  Mapping operations share common input parameters ``m``, ``in_col``, and
  ``out_col``.

  ``in_col``: The name or numerical index of the column in the input data file.

  ``out_col``: The name of the standard concept to use for the output column in
  the DataFrame containing extracted data.

  ``m``: The magic (or the "map"). Depending on the mapping operation, this
  will be a dict, function, or constant value.

.. sidebar:: When to use keep_map

    You have a column of file names that you want to preserve exactly.

Keep Map
--------

.. code-block:: python

    def keep_map(in_col, out_col):

The ``keep_map`` operation copies all of the values from ``in_col`` to
``out_col`` without modifying the values.

``keep_map`` is a special case of ``value_map`` with ``m=lambda x: x``.

.. sidebar:: When to use constant_map

    You know that all of your genomic files came from Whole Genome Sequencing.

Constant Map
------------

.. code-block:: python

    def constant_map(m, out_col):

The ``constant_map`` operation writes `N` iterations of constant value ``m``
to ``out_col``, where `N` is the number of rows in the source data table.

``constant_map`` is a special case of ``value_map`` with ``m=lambda x: <some
constant>``.

.. role:: underline
    :class: underline

.. caution::

    Only use ``constant_map`` for :underline:`columns that are not in the
    data`. Why? What if the column is 99% one value, so you think "I'll just
    constant map to that value", but then you change the other 1% of values
    accidentally because you never saw them? Or what if the next version of the
    data introduces a new value? Your error. Don't do it. Use ``value_map``
    instead for existing columns.

.. sidebar:: When to use value_map

    Your data uses 1/2/3/4 instead of Male/Female/Other/Unknown.

Value Map
---------

.. code-block:: python

    def value_map(m, in_col, out_col):

The ``value_map`` operation takes values from ``in_col``,
performs some transformation ``m`` on them, and then writes the
result to ``out_col`` in your result dataframe.

``m`` can be a function, dict, or regex pattern string:

* If it's a function, the returned column values will be the result of applying
  that function to each value in ``in_col``.

* If it's a dict, each key is a regex pattern string, and each value is either
  a direct replacement or a function applied to values in ``in_col`` matching
  the pattern.

* If it's a regex pattern string, the result is the same as if it had been
  ``{m: lambda x: x}``.

.. note::

  * Regex patterns without ``^`` and ``$`` anchors will have them automatically
    added.
  * ``m`` dict regex pattern matches are checked in order, and values from 
    ``in_col`` are each only matched once, so ``{"a": "b", "b": "c"}`` will 
    never convert "a" into "c".
  * If a regex pattern includes capture groups, the receiving function will
    receive only the captures instead of the whole cell value.

.. sidebar:: When to use row_map

    Your data has one column for folder path and one for filename but you need
    a column that combines both into a full file path.

Row Map
-------

.. code-block:: python

    def row_map(m, out_col):

The ``row_map`` operation lets you build a single output column using data from
across entire rows of source data when your output column needs to combine data
from multiple cells in the row. ``m`` is a function that takes an entire row as
input and returns a single value using values from that row.

.. sidebar:: When to use column_map

    You have an input column where some cells just say "ditto", meaning that
    you need to propagate values from the cells above.

Column Map
----------

.. code-block:: python

    def column_map(m, in_col, out_col):

The ``column_map`` operation lets you pass an entire column of data into a
custom function ``m`` which returns a new column (of the same length) as the
result.

.. note:: Needing to use ``column_map`` is a sign that the original data is
  poorly constructed.

.. sidebar:: When to use melt_map

    Column headers are phenotype names, and cell values are +/- for
    each name for each person, and you need to get one column with names and
    one column with Yes/No values.

Melt Map
--------

.. code-block:: python

    def melt_map(var_name, map_for_vars, value_name, map_for_values):

The ``melt_map`` operation combines the pandas melt function and the
``value_map`` operation. It both reshapes data and maps values.

* ``var_name`` is the melt's resulting var column name.

* ``map_for_vars`` is a dict with keys equal to column names or numeric indices
  from the source data and values equal to replacement var names.

* ``value_name`` is the melt's resulting value column name.

* ``map_for_values`` is the same as ``value_map``'s ``m``.

.. note::

  Any use of ``melt_map`` can be replaced by multiple uses of ``value_map``
  with appropriate column stacking.

.. sidebar:: When to use df_map

    When you have no other option.

DF Map
------

.. code-block:: python

    def df_map(m):

The ``df_map`` operation is for when you just can't figure out how to do what
you want to do with the other predefined operations. Take a whole DataFrame,
perform some function ``m`` on it, and return a new DataFrame of the same size.

.. note::

    You'll probably never use ``df_map``. That's fine. It's there for geometric
    completeness. Needing to use it is a sign that the original data is poorly
    constructed.
