#!/bin/bash

if [[ $KF_INGEST_APP_MODE = "production" ]]; then
    echo "Loading production app settings into environment ..."

    if [[ -n $VAULT_ADDR ]] && [[ -n $VAULT_ROLE ]]; then
        vault login -method=aws role=$VAULT_ROLE 2>&1 | grep authent

        echo "Load secrets from vault ..."

        # Get secrets from vault
        VAULT_JSON=$(vault read -format=json 'secret/aws/kf-ingest-app/auth0')
        SECRETS=$(echo $VAULT_JSON | jq -r ".data | to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]")

        # Set env vars
        for s in $SECRETS; do
            while IFS=$'=' read -r key value
            do
                export "$key"="$value"
            done <<< "$s"
        done
    fi
fi

INGEST_ERROR=0

kidsfirst "$@"

if [[ $? -ne 0 ]]; then
    INGEST_ERROR=1
fi

# During deployment - rm ingest package after ingest completes
if [[ $CLEANUP ]]; then
    INGEST_PACKAGE_DIR="/data/packages/$CLEANUP"
    if [[ -d $INGEST_PACKAGE_DIR ]]; then
        echo "Start cleanup ..."
        echo "Deleting ingest package $INGEST_PACKAGE_DIR"
        rm -rf $INGEST_PACKAGE_DIR
        echo "Cleanup Complete"
    fi
fi

if [[ $INGEST_ERROR -ne 0 ]]; then
    echo "Error in docker entrypoint exiting with 1!"
    exit 1
fi
