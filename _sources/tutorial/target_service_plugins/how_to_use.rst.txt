.. _Tutorial-Use-Target-Service-Plugin:

#############################
Using a Target Service Plugin
#############################

To use your target service plugin currently you need to bundle it together with
a new app settings file (described in :ref:`Tutorial-Ingest-App`), like this::

    my_plugin_dir/
    ├── my_app_settings.py
    ├── my_plugin.py
    └⋯⋯ other needed files like individual entity class builders

You will need to make your new app settings file reference your new target
service plugin by setting ``TARGET_API_CONFIG=<file path of your plugin>``.

Here's a simple example of what a new app settings file might look like:

.. code-block:: Python

    import os
    from kf_lib_data_ingest.app.settings.development import *

    TARGET_API_CONFIG = os.path.join(
        os.path.dirname(__file__), "my_plugin.py"
    )

Once your plugin and app settings files are bundled together, you will tell the
ingest library to use your new app settings when ingesting your next study by
running it with the ``--app_settings`` argument. Something like this:

.. code-block:: shell

    kidsfirst ingest --app_settings my_plugin_dir/my_app_settings.py ...
