.. _Value-Principles:

================
Value Principles
================

The ingest library operates on a few key principles:

* All data can be unambiguously represented as a set of tables where values in
  the same table row are fundamentally linked to each other.

* Some values act as identifiers of a thing, and some values act as attributes
  of an identified thing.

* Which thing is identified by an identifier is unambiguous.

* Relationships between identified things and their attributes and between
  different identified things are unambiguous.

* Each column represents something different.

* All occurrences of a particular identifier across all tables identify the
  same thing.

Practical Implications
----------------------

* Relationships between values may transitively span across multiple tables.

* Attributes of a thing need not be in the same table as the identifier(s) of
  the thing as long as there is an unambiguous path via some other linked
  thing.

* A table cell may contain no more than one value.

* Every cell value stands on its own, without secret reference to other cell
  values, and can be fully understood based exclusively on the column header
  and an optional data dictionary.

* The relationship between two column entries (in the same row) must be the
  same regardless of the row.

* Table normalization or denormalization doesn't matter, but full
  denormalization may not be possible or practical.

The Analyst's Challenge
-----------------------

Sometimes raw documents from investigators will appear to violate the
principles until their contents are repartitioned into multiple overlapping
tables. Sometimes raw document violations of the principles cannot be rectified
by repartitioning the data. Those documents are wrong and must be sent back. It
is the analyst's job to determine how to repartition the given documents and
when to send documents back.
