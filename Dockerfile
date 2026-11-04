FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    p7zip-full \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY uploader.py .

CMD ["python", "-u", "uploader.py"]
