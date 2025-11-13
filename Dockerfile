# Use official slim Python 3.12 image
FROM python:3.12-slim

# Avoid Python buffering for logs
ENV PYTHONUNBUFFERED=1

# Create app dir
WORKDIR /app

# Copy only what we need first for layer caching
COPY requirements.txt ./
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    ca-certificates \
    netcat-traditional \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy app files
COPY publisher.py ./
COPY config.yml ./
# If you have CSV/JSON data files referenced by config.yml, copy them too:
# COPY data/ ./data/

# Add a small script that waits for the broker to be reachable
COPY wait_for_broker.sh /usr/local/bin/wait_for_broker.sh
RUN chmod +x /usr/local/bin/wait_for_broker.sh

# Default env vars (overrideable in docker-compose or CLI)
ENV MQTT_BROKER_HOST=mosquitto
ENV MQTT_BROKER_PORT=1883

# Entrypoint runs the waiter then the publisher
ENTRYPOINT ["/usr/local/bin/wait_for_broker.sh"]
CMD ["python", "publisher.py", "config.yml"]
