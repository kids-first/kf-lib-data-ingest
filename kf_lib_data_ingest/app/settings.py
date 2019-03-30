"""
Default app level settings for ingest CLI app

App has 3 operation modes: `development`, `testing, ``production`
The mode can be changed via the environment variable `KF_INGEST_APP_MODE`
If this variable is not set, then default to `development` mode

Settings for each mode are encapsulated in a Settings subclass

In `development` mode
---------------------
- Used when developing the ingest app and you need to test against
locally running servers or deployed development servers

- User is responsible for setting the necessary environment variables
needed for authentication with source or target services

In `testing` mode
---------------------
- Used when testing ingest packages and you need to test against
production Kids First Study Creator API

- User is responsible for setting the necessary environment variables
needed for authentication with source or target services

In `production` mode
--------------------
- Used when CI testing ingest packages in a deployed environment

- ProductionSettings will login to Vault, fetch values for any environment
variables listed in it's auth_config.variable_names dict that have not yet been
set in the environment. Once successfully retrieved, it will set the
environment variables with the fetched values.
"""

import os
import hvac


from kf_lib_data_ingest.config import ROOT_DIR

APP_MODE_ENV_VAR = 'KF_INGEST_APP_MODE'


class Settings(object):
    """
    Base settings for Kids First CLI app
    """

    def __init__(self):
        self.target_api_config = os.path.join(ROOT_DIR,
                                              'target_apis',
                                              'kids_first.py')
        self.auth_config = {}


class DevelopmentSettings(Settings):
    """
    Settings for Kids First CLI app while in `development` mode
    """

    def __init__(self):
        super().__init__()
        self.auth_config.update(
            {
                'http://localhost:8080/download/study': {
                    'type': 'token',
                    'token_location': 'url',
                    'variable_names': {
                        'token': 'KF_STUDY_CREATOR_API_TOKEN',
                    }
                },
                'http://kf-study-creator-dev.kids-first.io/download/study': {
                    'type': 'token',
                    'token_location': 'url',
                    'variable_names': {
                        'token': 'KF_STUDY_CREATOR_API_TOKEN',
                    }
                }
            }

        )


class TestSettings(Settings):
    """
    Settings for Kids First CLI app while in `testing` mode
    """

    def __init__(self):
        super().__init__()
        self.auth_config.update(
            {
                'https://kf-study-creator.kidsfirstdrc.org/download/study': {
                    'type': 'token',
                    'token_location': 'url',
                    'variable_names': {
                        'token': 'KF_STUDY_CREATOR_API_TOKEN',
                    }
                }
            }
        )


class ProductionSettings(Settings):
    """
    Settings for Kids First CLI app while in `production` mode
    """

    def __init__(self):
        super().__init__()
        self.auth_config.update(
            {
                'https://kf-study-creator.kidsfirstdrc.org/download/study': {
                    'type': 'oauth2',
                    'variable_names': {
                        'provider_domain': 'KF_AUTH0_DOMAIN',
                        'audience': 'KF_STUDY_CREATOR_AUTH0_AUD',
                        'client_id': 'KF_SC_INGEST_APP_CLIENT_ID',
                        'client_secret': 'KF_SC_INGEST_APP_CLIENT_SECRET'
                    }
                }
            }
        )

        os.environ['KF_AUTH0_DOMAIN'] = 'kidsfirst.auth0.com'
        os.environ['SC_AUTH0_AUD'] = 'kf-study-creator-api.kids_first.io'

        # Login to vault
        vault_url = os.environ.get('VAULT_URL', 'https://vault:8200/')
        vault_role = os.environ.get('VAULT_ROLE', 'IngestAppRole')
        client = hvac.Client(url=vault_url)
        client.auth_iam(vault_role)

        # For any env vars that are not set yet in auth config,
        # try getting them from vault and set them in the environment
        for url, config in self.auth_config.items():
            for env_var in config.values():
                if not os.environ.get(env_var):
                    val = client.read(
                        f'secret/kf_ingest_app/{env_var.lower()}')
                    os.environ[env_var] = val
        client.logout()


def setup():
    """
    Select the appropriate app Settings class based on the value of
    the environment variable `KF_INGEST_APP_MODE`

    :returns an instance of Settings class
    """
    settings_dict = {
        'development': DevelopmentSettings,
        'testing': TestSettings,
        'production': ProductionSettings
    }
    app_mode = os.environ.get(APP_MODE_ENV_VAR, 'development')
    app_settings_cls = settings_dict.get(app_mode)

    return app_settings_cls()
