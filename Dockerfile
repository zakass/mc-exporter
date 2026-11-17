FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1 \
    MC_LOGS_DIR=/mc-logs \
    MC_CONTAINER_NAME=minecraft \
    MC_HOST=minecraft \
    MC_PORT=25565 \
    DOCKER_SOCK=/var/run/docker.sock \
    STATE_DB_PATH=/state/mc_exporter.db \
    LISTEN_HOST=0.0.0.0 \
    LISTEN_PORT=9100 \
    DOCKER_POLL_INTERVAL=5 \
    LOG_POLL_INTERVAL=1 \
    SLP_POLL_INTERVAL=10 \
    BACKFILL_LOGS=true

WORKDIR /app

COPY exporter.py /app/exporter.py

EXPOSE 9100

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9100/healthz', timeout=3).read()" || exit 1

CMD ["python3", "/app/exporter.py"]
