FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y make git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
COPY . /app/
RUN pip install --no-cache-dir -e .[dev]
