FROM python:3.11-slim

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# copy requirements & install
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

# copy app
COPY app_docker.py .

EXPOSE 8501
ENV STREAMLIT_SERVER_ENABLECORS=false
CMD ["streamlit", "run", "app_docker.py", "--server.port=8501", "--server.address=0.0.0.0"]
