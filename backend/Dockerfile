FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_DEBUG=0 \
    FLASK_RUN_HOST=0.0.0.0 \
    FLASK_RUN_PORT=8000 \
    GUNICORN_WORKERS=4 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=120

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

EXPOSE 8000

# Shell form so the worker/thread/timeout counts can be tuned via env vars
# (exec keeps gunicorn as PID 1 for clean signal handling).
CMD exec gunicorn --bind 0.0.0.0:8000 \
    --workers "${GUNICORN_WORKERS:-4}" \
    --threads "${GUNICORN_THREADS:-4}" \
    --timeout "${GUNICORN_TIMEOUT:-120}" \
    run:app
