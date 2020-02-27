FROM python:3

ENV KF_INGEST_APP_MODE production

WORKDIR /app

RUN apt-get update && apt-get install -y jq wget && \
    wget -q -O vault.zip https://releases.hashicorp.com/vault/1.0.3/vault_1.0.3_linux_amd64.zip && \
    unzip vault.zip && \
    mv vault /usr/local/bin

COPY . /app

RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -e .

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
