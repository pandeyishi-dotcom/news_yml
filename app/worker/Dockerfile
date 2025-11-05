FROM python:3.11-slim

WORKDIR /usr/src/worker

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt

COPY worker.py .

CMD ["python", "worker.py"]
