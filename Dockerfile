# syntax=docker/dockerfile:1

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
# so that logs surface immediately in `fly logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first to maximize layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source.
COPY dca5_bot.py .

# Informational only — Fly.io injects the actual PORT environment variable
# at runtime and routes to it based on the internal_port setting below.
EXPOSE 8080

# Create the mount point for the persistent volume and a non-root user,
# then hand ownership of the mount point to that user so the app can
# write its trigger-state file at runtime.
RUN mkdir -p /data && \
    useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /data

USER appuser

CMD ["python", "dca5_bot.py"]
