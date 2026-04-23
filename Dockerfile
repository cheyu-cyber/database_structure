FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY services/ ./services/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/services

# Each compose service overrides CMD with its own entrypoint module.
CMD ["python", "-c", "print('Override CMD in docker-compose.yml')"]
