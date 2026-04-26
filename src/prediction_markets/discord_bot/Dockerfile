# Discord bot Cloud Run service. Built by Cloud Build from /cloudbuild.yaml,
# same trigger as the debate-worker. Built from the repo root.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN pip install --upgrade pip && pip install ".[discord_bot]"

ENV PORT=8080
EXPOSE 8080

# Discord requires a response within 3s. We keep gunicorn workers low and rely
# on Cloud Run autoscaling for concurrency — the BQ thread lookup + Pub/Sub
# publish complete well under the limit in our common path.
CMD exec gunicorn \
    --bind ":${PORT}" \
    --workers 2 \
    --threads 4 \
    --timeout 30 \
    prediction_markets.discord_bot.server:app
