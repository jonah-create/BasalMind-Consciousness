# Observer Entity - Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY Observer/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install NATS client
RUN pip install --no-cache-dir nats-py

# Copy Observer package
COPY Observer/observer/ ./observer/

# Create log directory
RUN mkdir -p /var/log/observer

# Environment variables (overridable)
ENV OBSERVER_HOST=0.0.0.0
ENV OBSERVER_PORT=5001
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:5001/health', timeout=2)"

# Run Observer
CMD ["python", "-m", "observer.main"]
