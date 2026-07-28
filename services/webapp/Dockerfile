# Build context is the riskradar/ root so we can install the shared library.
FROM python:3.11-slim

WORKDIR /app

COPY services/webapp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY libs/core /libs/core
RUN pip install --no-cache-dir /libs/core

COPY services/webapp/src /app/src
# tools/ rides along so the seed-baseline one-shot can share this image.
COPY tools /app/tools
RUN pip install --no-cache-dir -r /app/tools/requirements.txt

ENV PYTHONPATH=/app/src:/app/tools \
    PYTHONUNBUFFERED=1

EXPOSE 3000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3000", "--app-dir", "/app/src"]
