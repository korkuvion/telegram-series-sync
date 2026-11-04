FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends p7zip-full \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY uploader.py .

STOPSIGNAL SIGTERM
CMD ["python", "uploader.py"]
