"""
Default app level settings for ingest CLI app

App has 3 operation modes:
    - development
    - testing
    - production

The app mode can be changed via the environment variable `KF_INGEST_APP_MODE`
If this variable is not set, then default to `development` mode

Default settings for each mode are encapsulated in Python modules
inside this package, each named `{app_mode}.py`.

In `development` mode
---------------------
- Used when developing the ingest app and you need to test against
locally running servers or deployed development servers

- User is responsible for setting the necessary environment variables
needed for ingest (i.e. authentication with other services)

In `testing` mode
---------------------
- Used when testing ingest packages and you need to test against
production Kids First Study Creator API

- User is responsible for setting the necessary environment variables
needed for ingest (i.e. authentication with other services)

In `production` mode
--------------------
- Used when CI testing ingest packages in a deployed environment

- Ingest app will run inside a Docker container

- Docker scripts/entrypoint.sh script is responsible for setting necessary
environment variables needed for ingest (i.e. authentication with
other services)
"""
import os

from kf_lib_data_ingest.common.misc import import_module_from_file

APP_MODE_ENV_VAR = 'KF_INGEST_APP_MODE'


def load(app_settings_filepath=None):
    """
    Import the app settings Python module

    If `app_settings_filepath` is not supplied, load the appropriate
    default settings module based on the value of the environment variable
    `KF_INGEST_APP_MODE`.

    :param app_settings_filepath: path to app settings Python module
    :type app_mode: str

    :returns imported settings module
    """
    # App mode of operation
    app_mode = os.environ.get(APP_MODE_ENV_VAR) or 'development'

    # Load default app settings for the app mode
    if not app_settings_filepath:
        app_settings_filepath = os.path.join(
            os.path.dirname(__file__), app_mode) + '.py'

    # Import module
    app_settings = import_module_from_file(app_settings_filepath)
    setattr(app_settings, 'APP_MODE', app_mode)
    setattr(app_settings, 'FILEPATH', app_settings_filepath)

    return app_settings
