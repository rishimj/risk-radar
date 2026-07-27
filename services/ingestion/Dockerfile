# Build context is the riskradar/ root so we can install the shared library.
FROM python:3.11-slim

WORKDIR /app

COPY libs/core /libs/core
RUN pip install --no-cache-dir /libs/core

COPY services/ingestion/src /app/src

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

CMD ["python", "/app/src/main.py"]
