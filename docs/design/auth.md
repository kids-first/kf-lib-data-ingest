# Authentication/Authorization to Access Source Data

**\* Ingest App**

Here we will refer to the ingest app as the CLI which utilizes ingest library
to run ingestions. Its the thing that will be Dockerized into a self contained bundle
and have IAM policies attached to it so that it can fetch secrets from
Vault (which is used for Kids First secrets management).

## Which auth types should the ingest library support?
We need to decide which types of authentication mechanisms the ingest library
will support, how and when credentials, secrets get read in and
supplied to FileRetriever which fetches source data files.

## OAuth2
We at least need to support OAuth 2 because kids first study files are being
stored and fetched from study creator API, which uses Ego, an OAuth2 service
that authorizes clients requesting access to study creator resources.

With OAuth2 the following flow would take place:

 1. The ingest app will need to be registered with the OAuth2 service to
 obtain a client_id and client_secret

 2. The client id and secret will need to be stored securely -
 probably in Vault.

**Notes**

    - The steps above could be done dynamically at ingest runtime or if that is
    not possible they will need to be done ahead of time for each anticipated
    OAuth2 provider.

    - In order for ingest app to fetch (and even maybe create) client id, secret
    pairs from Vault, it will require adding a policy for ingest app
    to the aws vault playbook.

3. The ingest app will fetch its client_id and client_secret from vault

4. The ingest app will auth with the OAuth2 service to get a service token

5. Then the ingest app will auth with source data service using the provided
  OAuth2 service token so that it may fetch files or other protected resources
  from the source data service.


# Questions

## OAuth 2 Grant Type
Can ingest app use Client Credentials grant type?
Auth0 recommends Client Credentials grant type for machine to machine auth.
Use cases for this grant type include CLIs, daemons, or services.

### How will auth type, credentials, and auth provider be supplied to ingest app?
Right now only the source data file URL is stored in the extract config for a
particular file. If the file URL requires auth of any kind how should that
be specified/encoded?

Ingest app will need to know:
- the auth type (whichever we support)
- what authentication credentials to load in and pass to FileRetriever.
- since file URLs may require authentication with one or more different OAuth 2  
providers (Ego, Auth0, whatever else), ingest app will also need to know
which OAuth 2 provider to authenticate with when fetching an OAuth2 protected
resource.

### Auth Configuration

```yaml
authentication:
    http://www.example.com/files:
        type: basic
        variable_names:
            username: EXAMPLE_SERVICE_UNAME
            password: EXAMPLE_SERVICE_PW        

    http://api.com/files:
        type: OAuth2
        variable_names:
            provider: EGO_URL
            client_id: CLIENT_ID_EGO
            client_secret: CLIENT_SECRET_EGO
```
- Putting this in the extract configs would be redundant and not appropriate
- Could go into ingest package config but even that may be redundant and would
require the ingest package developer to specify things they maybe should not
have to worry about

### App Config File
This could be defined inside an app level config file, and passed into the
ingest app at runtime. If its not specified, a default app config file thats
part of the library could be used.

Other things that might later go in here: the target API config to use,
the concept schema to use, etc.

```
kidsfirst ingest <ingest package> --app_config <path to my_app_config.yaml>

kidsfirst ingest <ingest package>  # results in default app_config.yaml being used
```

### Auth Environment Vars Not Set

**Production Mode**

Before ingestion, the vars in `variable_names` will be read in from Vault and written
to shell variables or some runtime config object accessible to pipeline during run.

During ingestion, if the variables' values are empty,
it will result in an error and stop ingestion.

**Development/Testing Mode**

Before ingestion, the vars in `variable_names` will be read in from the environment and written
to shell variables or some runtime config object accessible to pipeline during run.

During ingestion, if the variables' values are empty,
it will result in an error and stop ingestion

## OAuth 2 Client Registration

- What is required to get a client id and secret? Usually its things like:
    - Application name
    - URL to the application’s home page
    - A short description of the application
    - etc
- What should ingest app supply for these things?

### How will auth affect FileRetriever?
We will probably need an auth.py Python module which processes the
extract config first so that it may create the auth object and
pass the correct inputs to FileRetriever.

FileRetriever should probably use the requests-oauthlib for fetching
OAuth 2 resources. FileRetriever may need an additional web getter
for this or the usage of requests-oauthlib should be inside the existing
web getter.
