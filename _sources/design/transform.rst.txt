===============
Transform Stage
===============

The transform stage is where data from all of the individual extracted source
files is blended together to form a holistic representation of the study using
knowledge of the concept associations needed to fully represent entities in
your data (e.g. Participants, Biospecimens, that sort of thing).

In this stage you merge the extracted dataframes together into either one large
denormalized table (the "default" output) or into smaller subset tables where
each row in a subset table fully represents one entity.

.. note::
    It should be possible to one day fully automate this process according to
    formal rules about how entities relate to each other in your target service,
    but for now that is left as an exercise for the reader.
