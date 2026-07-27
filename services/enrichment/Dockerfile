# Build context is the riskradar/ root so we can install the shared library.
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# CPU-only torch: the default index pulls the CUDA build, which is ~2.5GB of
# wheels we would never execute in this stack.
COPY services/enrichment/requirements.txt .
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
    -r requirements.txt

COPY libs/core /libs/core
RUN pip install --no-cache-dir /libs/core

COPY services/enrichment/src /app/src

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/models

EXPOSE 8082

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8082", "--app-dir", "/app/src"]
