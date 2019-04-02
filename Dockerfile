FROM python:3

ENV KF_INGEST_APP_MODE production

WORKDIR /app

COPY setup.py requirements.txt dev-requirements.txt /app/

RUN pip install --upgrade pip && \
    # pip install -r dev-requirements.txt && \
    pip install --no-cache-dir -e .

COPY . /app

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
