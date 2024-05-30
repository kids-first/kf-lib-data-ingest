.. _samples_and_specimens:

========================
Samples and Biospecimens
========================

Although similarly named, samples and biospecimens refer to different concepts:

    * A **sample** represents a physical piece of tissue, blood, or other
      biologically distinct material taken from a patient.
    * A **biospecimen** is represents a portion or a part of that sample, e.g.
      an aliquot of a sample.

While samples and biospecimens are distinct concepts, they share much in
common. In fact, when the ingest library was first written, its primary target
API, the Kids First Data Service, only had a table for biospecimens. As a
result, the ingest library's architecture provides for a biospecimen to share
*all* the qualities of a sample. In fact, biospecimen is a child class of
sample!

This architecture allows the ingest library to be used against target APIs
that, like the older versions of the Kids First Data Service, only have a table
for biospecimens.

A sample has qualities:

    * A sample may have information about itself, such as the type of tissue it
      is, the type of tumor it comes from, when the sample was collected from
      the participant, its volume, etc.
    * A sample may have information about shipping, such as the date it was
      shipped and shipment origin


As discussed above, a biospecimen is a child class of sample, so biospecimens
may have all of the same qualities of a sample*. In addition:

    * a biospecimen may have information about its concentration
    * a biospecimen may have information about its analyte type (e.g. DNA vs
      RNA)
    * a biospecimen may have information about the consent under which it was
      collected.

Biospecimen is designed as a child class of sample to provide for
backwards-compatibility with older ingest packages that existed before the
sample concept.

Moving forward, it is advised to use the sample class when
extracting information that is most related to the sample and use biospecimen
only when extracting information that is specific to the biospecimen
(such as concentration, analyte, and consent information).
