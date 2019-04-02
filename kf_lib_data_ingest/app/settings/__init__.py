"""
Default app level settings for ingest CLI app

App has 3 operation modes:
    - development
    - testing
    - production

The app mode can be changed via the environment variable `KF_INGEST_APP_MODE`
If this variable is not set, then default to `development` mode

Settings for a mode are encapsulated in a Python module named `{app_mode}.py`
inside this package.

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


def load(app_mode=None):
    """
    Select and import the appropriate app settings module based on the value of
    `app_mode` if set or the environment variable `KF_INGEST_APP_MODE`

    :param app_mode: mode of operation for app
    :type app_mode: str

    :returns selected settings module
    """
    if not app_mode:
        app_mode = os.environ.get(APP_MODE_ENV_VAR, 'development')

    fp = os.path.join(os.path.dirname(__file__), app_mode) + '.py'
    app_settings = import_module_from_file(fp)
    setattr(app_settings, 'APP_MODE', app_mode)

    return app_settings
