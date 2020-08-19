FROM blackdentech/kf-pandas

ENV KF_INGEST_APP_MODE production

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -e . \
    pip install awscli

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
