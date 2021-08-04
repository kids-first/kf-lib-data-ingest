# Adding Ingest Genomic Workflow Output functionality

**Status:** Ready for comments!

## Overview
The center receives unharmonized genomic files and then the BIX team puts
them through a harmonization process, which produces harmonized genomic files.
A spreadsheet providing details about this process and the results is produced
which we refer to as the Genomic Workflow Output (GWO) manifest. This
manifest includes data that should be loaded into the DataService. As of now
this is done manually by analysts through a time-intensive process.

Work has been done recently on the Study Creator API to allow
a DataTracker user to automatically perform the
Ingest GWO process after uploading a manifest by pushing an
`ingest` button. A working solution was developed, but
initiating a complex process with just a button press and without detailed
monitoring and feedback is not ideal. Creating the appropriate machinery to
allow for substantial monitoring of the process would require prolonged effort
on the frontend.

In the meantime, the desired backend functionality is there but unable
to be used in its current form. While this work was done on
the Study Creator API, it can really be viewed as just an extension of the
ingest library. It would save analysts a lot of time if this functionality was
added to the ingest library and could be completed using the CLI.
This sidesteps the need for frontend development and
also has the benefit of allowing the operation to be fine-tuned before
potentially being exposed to a wider audience.

## Ingest GWO process
After the usual ingestion process for a study occurs, the DataService should
be setup for the Ingest GWO process. We need
unharmonized genomic files, sequencing experiments, sequencing experiment -
genomic file links, and biospecimen - genomic file links for the study to be
present in the DataService. Assuming this is true, we can proceed with the
ingest GWO process, which looks like this:

1. Ingest genomic file metadata (such as size, hashes, etc) at the DataService
endpoint `/genomic-files`. Metadata for each genomic file in the manifest will
need to be scraped from S3, requiring active and valid creds.
2. Ingest harmonized genomic file - biospecimen links at the DataService
endpoint `/biospecimen-genomic-files`. All necessary information for this step
will be in the manifest.
3. Ingest harmonized genomic file - sequencing experiment links at the
DataService endpoint `/sequencing-experiment-genomic-files`. This is where
most of the work is done. We've split it up into the
following sub-steps (assume our study's ID is `foo`):
    1. Get the study's unharmonized genomic files (DataService query
    `/genomic-files?study_id=foo&is_harmonized=False&visible=True`)
    2. Get the study's sequencing experiments (DataService query
    `/sequencing-experiments?study_id=foo&visible=True`)
    3. Get the study's sequencing experiment - genomic file links (DataService
    query `/sequencing-experiment-genomic-files?study_id=foo&visible=True`)
    4. Get the study's biospecimen - genomic file links (DataService query
    `/biospecimen-genomic-files?study_id=foo&visible=True`)
    5. Join the tables from i, ii, iii , iv on genomic file ID column
    6. Join the table from the previous step with the manifest on
    biospecimen ID.
This yields a table with sequencing experiment information about each
harmonized file in the manifest.

## Addition to Ingest library
The code for this turned out to be more complex than
expected. Adding it to the ingest library in a manner which meets its high
standards will probably not be trivial and require
refactoring. It is essential that we characterize how we want this new
functionality to look and operate. Or at least it is essential that we start
with some concrete ideas and then fine tune them appropriately based on 
received feedback from the end users.

### Default functionality
What would the operation default to? What options should we provide? How will
these impact the result?

### CLI implementation
Are there any things we need to watch out for in the transition from the code
as it stands now to a submodule of the ingest library?

### Monitoring/Reporting
As detailed above this process requires multiple steps. What can we do to
monitor the process and determine the status of each step? What would be the
best way to report what happened and the results? Would there be any
difference between a dry run and a normal run other than whether data gets
loaded into a warehouse or not?

### Error handling
What are the common classes of errors that we might expect? Are any of these
common errors able to be ignored? How should the process react to the various
types and classes of errors? What can we do to catch logical errors and
prevent them from reaching production?

Example errors:
- The user tries to run the process on a file that isn't a GWO manifest
- The GWO manifest is incomplete or incorrect (maybe typos or wrong data)
- Items that the GWO manifest references don't exist in the DataService (maybe
a biospecimen ID included in the manifest doesn't exist)
- etc.

### Validation
Do we want to perform validation on the resultant data? If so, what might
that look like?
