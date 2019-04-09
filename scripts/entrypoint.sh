#!/bin/bash

if [[ $KF_INGEST_APP_MODE = "production" ]]; then
    echo "Loading production app settings into environment ..."

    if [[ -n $VAULT_ADDR ]] && [[ -n $VAULT_ROLE ]]; then
        vault login -method=aws role=$VAULT_ROLE 2>&1 | grep authent

        echo "Load secrets from vault ..."

        # Get secrets from vault
        vault_json = $(vault read -format=json "secret/aws/kf-ingest-app/auth0")
        secrets=$(echo $vault_json | jq -r ".data.data | to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]")

        # Set env vars
        for s in $secrets; do
            while IFS=$'=' read -r key value
            do
                export "$key"="$value"
            done <<< "$s"
        done
    fi
fi

kidsfirst "$@"
