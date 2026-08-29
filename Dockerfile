FROM python:3.9-slim
WORKDIR /app

# Install system dependencies including libmagic
RUN apt-get update && apt-get install -y \
    git \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/config /app/data /app/logs

# Set permissions
RUN chmod +x *.py

CMD ["python", "skeeter_deleter.py"]
