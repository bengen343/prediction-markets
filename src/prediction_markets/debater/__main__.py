"""Local-run entry point. In production the Cloud Run container invokes
gunicorn directly against `prediction_markets.debater.server:app`.
"""
import os

from .server import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
