"""Local-run entry. In production gunicorn invokes the Flask app directly."""
import os

from .server import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
