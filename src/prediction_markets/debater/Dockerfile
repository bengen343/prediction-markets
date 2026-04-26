# Debater Cloud Run service. Built by Cloud Build on every push to main that
# touches src/prediction_markets/debater/** or this Dockerfile (see
# /cloudbuild.yaml for the trigger config). Built from the repo root.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

# Install the package along with its debater extras. Cloud Run's filesystem is
# read-only at runtime, so install at build time.
RUN pip install --upgrade pip && pip install ".[debater]"

# Cloud Run injects PORT (default 8080). One worker, one thread — we enforce
# serial debate processing at this layer too so a misconfigured concurrency
# setting doesn't accidentally fan out.
ENV PORT=8080
EXPOSE 8080

CMD exec gunicorn \
    --bind ":${PORT}" \
    --workers 1 \
    --threads 1 \
    --timeout 600 \
    prediction_markets.debater.server:app
