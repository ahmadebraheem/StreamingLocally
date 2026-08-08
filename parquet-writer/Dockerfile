# =============================================================================
# Parquet Writer Docker Image
# =============================================================================
# Long-running consumer that reads from Kafka topics "processed" and
# "raw-events-dlq", batches records, and writes Apache Parquet files to disk.
#
# Output directory: /app/parquet-output (bind-mounted to ./parquet-output on host)
# Depends on:       kafka service (must be reachable at kafka:9092)
#
# Build context: ./parquet-writer (see docker-compose.yml service "parquet-writer")
# =============================================================================

FROM python:3.13-slim

WORKDIR /app

# Install kafka-python (consumer) and pyarrow (Parquet serialization)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY writer.py .

CMD ["python", "writer.py"]
