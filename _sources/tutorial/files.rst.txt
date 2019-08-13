Study Data Files
================

If you are developing an ingest package for Kids First study data, then the
source data files for the study will be managed by the Kids First Study
Creator. Read more about the Study Creator API at
https://kids-first.github.io/kf-api-study-creator/

Upload Files
------------

Source data files must be uploaded to an existing Kids First study in the Study
Creator. The easiest way to do this is via the Kids First Data Tracker web
app at https://kf-ui-data-tracker.kidsfirstdrc.org/

Currently, new studies must be created first via the Kids First Dataservice
API, and they will then be mirrored/synced in the Study Creator.

Download Files
--------------

Once files are uploaded into a study, they will be accessed via the Study
Creator API's file endpoint with an access token in the authorization header
of the request.

You will learn how to configure your ingest package to access these files in
the :ref:`Tutorial-Extract-Stage` section of the tutorial.
