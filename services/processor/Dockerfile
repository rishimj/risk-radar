# The lite stream engine. Everything it needs (kafka-python, redis, sqlalchemy,
# requests) comes in with riskcore, so this image is ~200MB and native on arm64,
# versus the Flink images which run amd64 under Rosetta on an M-series Mac.
# Build context is the riskradar/ root so we can install the shared library.
FROM python:3.11-slim

WORKDIR /app

COPY libs/core /libs/core
RUN pip install --no-cache-dir /libs/core

COPY services/processor/src /app/src

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

EXPOSE 8083

CMD ["python", "/app/src/main.py"]
