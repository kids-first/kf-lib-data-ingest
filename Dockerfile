FROM python:3

ENV KF_INGEST_APP_MODE production

WORKDIR /app

COPY setup.py requirements.txt dev-requirements.txt /app/

RUN pip install --upgrade pip && \
    # pip install -r dev-requirements.txt && \
    pip install --no-cache-dir -e .

RUN apt-get update && apt-get install -y jq wget && \
    wget -q -O vault.zip https://releases.hashicorp.com/vault/1.0.3/vault_1.0.3_linux_amd64.zip && \
    unzip vault.zip && \
    mv vault /usr/local/bin

COPY . /app

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
