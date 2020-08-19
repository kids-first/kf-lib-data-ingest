#!/bin/bash

set -e

if [[ $KF_INGEST_APP_MODE = "production" ]]; then
    echo "Loading production app settings into environment ..."

    SECRETS=$(aws s3 cp s3://kf-strides-232196027141-us-east-1-prd-secrets/kf-lib-data-ingest/secrets.env -)

    for s in $SECRETS; do
        export "$s"
        done
    done
    
fi

kidsfirst "$@"
