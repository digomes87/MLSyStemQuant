
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (e.g., for librdkafka)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Environment variable to ensure Python output is unbuffered
ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
