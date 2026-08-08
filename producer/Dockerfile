# =============================================================================
# Producer API Docker Image
# =============================================================================
# Builds the FastAPI + aiokafka event producer service.
#
# Exposed port: 8000 (HTTP)
# Depends on:   kafka service (must be reachable at kafka:9092)
#
# Build context: ./producer (see docker-compose.yml service "producer-api")
# =============================================================================

FROM python:3.13-slim

WORKDIR /app

# Install Python dependencies first (layer caching: requirements change less often)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app.py .

# Start the ASGI server; bind to all interfaces so Docker port mapping works
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
