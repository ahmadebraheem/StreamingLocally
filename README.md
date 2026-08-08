# StreamingLocally

An end-to-end **on-premises streaming data pipeline** demo built entirely with Docker Compose. The stack ingests synthetic JSON events through Kafka, validates and routes them with Apache Flink, and archives the results as Apache Parquet files on local disk — no cloud services required.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Usage Guide](#usage-guide)
- [Kafka Topics](#kafka-topics)
- [Message Schemas](#message-schemas)
- [Flink Job Details](#flink-job-details)
- [Parquet Output](#parquet-output)
- [Querying Results](#querying-results)
- [Project Structure](#project-structure)
- [Configuration Reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)

---

## Overview

StreamingLocally demonstrates a production-style streaming pattern on a single machine:

1. **Ingest** — A FastAPI producer generates synthetic sensor-like events and publishes them to Kafka.
2. **Process** — An Apache Flink job validates each record, normalizes the schema, and splits failures into a dead-letter queue (DLQ).
3. **Archive** — A Python consumer reads both the happy-path and DLQ topics and writes batched Apache Parquet files to disk.

The pipeline intentionally injects malformed messages (~50% during burst mode) so you can observe DLQ routing, checkpoint recovery, and multi-topic consumption in action.

---

## Architecture

```
┌─────────────────┐     POST /start, /produce
│   HTTP Client   │──────────────────────────────────────┐
└─────────────────┘                                      │
                                                           ▼
                                              ┌────────────────────────┐
                                              │     producer-api       │
                                              │   (FastAPI + aiokafka) │
                                              └───────────┬────────────┘
                                                          │
                                                          │  test-topic
                                                          ▼
                                              ┌────────────────────────┐
                                              │        Kafka           │
                                              │   (KRaft, single node) │
                                              └───────────┬────────────┘
                                                          │
                                                          ▼
                                              ┌────────────────────────┐
                                              │   Flink JobManager     │
                                              │   + TaskManager        │
                                              │                        │
                                              │  ValidateAndSplit      │
                                              │  (PyFlink ProcessFn)   │
                                              └──────┬─────────┬───────┘
                                                     │         │
                              processed topic        │         │  raw-events-dlq topic
                                                     ▼         ▼
                                              ┌────────────────────────┐
                                              │        Kafka           │
                                              └───────────┬────────────┘
                                                          │
                                                          ▼
                                              ┌────────────────────────┐
                                              │    parquet-writer      │
                                              │  (kafka-python+pyarrow)│
                                              └───────────┬────────────┘
                                                          │
                                                          ▼
                                              ┌────────────────────────┐
                                              │   ./parquet-output/    │
                                              │   *.parquet files      │
                                              └────────────────────────┘
```

### Data flow summary

| Stage | Input | Output | Technology |
|-------|-------|--------|------------|
| Produce | HTTP request | `test-topic` messages | FastAPI, aiokafka |
| Validate | `test-topic` | `processed` + `raw-events-dlq` | Apache Flink 1.18, PyFlink |
| Archive | `processed`, `raw-events-dlq` | `.parquet` files | kafka-python, PyArrow |

---

## Components

### Kafka (`apache/kafka:3.8.0`)

Single-node broker running in **KRaft mode** (no ZooKeeper). Exposes:

| Port | Listener | Use |
|------|----------|-----|
| 9092 | PLAINTEXT | Inter-service communication inside Docker network |
| 9094 | EXTERNAL | Host-machine access (`localhost:9094`) for tools like `kcat` |

Topic data is persisted in a named Docker volume (`kafka-data`).

### Producer API (`producer/`)

FastAPI HTTP service that generates synthetic events.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/start?rate_per_sec=10` | POST | Begin continuous burst at given rate (50% bad messages) |
| `/stop` | POST | Halt the burst loop |
| `/produce` | POST | Send a single valid message |

Runs on **http://localhost:8000**. Interactive API docs at **http://localhost:8000/docs**.

### Flink Cluster (`flink-job/`)

Apache Flink 1.18.1 with PyFlink. Two containers share the same image:

- **jobmanager** — coordinates jobs, serves Web UI at **http://localhost:8081**
- **taskmanager** — executes operators (2 task slots)

Checkpoint state is stored in RocksDB and persisted to `./checkpoints/` on the host.

### Parquet Writer (`parquet-writer/`)

Long-running Kafka consumer that batches records and writes Parquet files to `./parquet-output/`. Flushes every **100 records** or **30 seconds**, whichever comes first.

---

## Prerequisites

- **Docker** 24+ with Docker Compose v2
- **curl** (or any HTTP client) for triggering the producer
- **8 GB RAM** recommended (Flink + Kafka are memory-hungry)
- Optional: **DuckDB**, **pandas**, or **Jupyter** for querying Parquet output

---

## Quick Start

```bash
# 1. Clone the repository and enter the project directory
git clone <repo-url> && cd StreamingLocally

# 2. Build and start all services
docker compose up --build -d

# 3. Wait ~30 seconds for Kafka and Flink to become healthy, then start producing events
curl -X POST "http://localhost:8000/start?rate_per_sec=20"

# 4. Submit the Flink validation job (one-time per stack lifecycle)
docker exec -it jobmanager ./bin/flink run -py /opt/flink/usrlib/job.py

# 5. After ~30-60 seconds, check for Parquet output
ls -la parquet-output/

# 6. Stop producing when done
curl -X POST http://localhost:8000/stop
```

---

## Usage Guide

### Producing events

**Burst mode** (continuous stream with intentional errors):

```bash
# Start at 20 messages/second (~50% will be malformed)
curl -X POST "http://localhost:8000/start?rate_per_sec=20"

# Stop the burst
curl -X POST http://localhost:8000/stop
```

**Single message** (always valid):

```bash
curl -X POST http://localhost:8000/produce
```

### Submitting the Flink job

The Flink job is **not** auto-submitted on startup. You must submit it manually:

```bash
docker exec -it jobmanager ./bin/flink run -py /opt/flink/usrlib/job.py
```

Monitor job status at http://localhost:8081 → **Running Jobs**.

To cancel a running job:

```bash
docker exec -it jobmanager ./bin/flink list
docker exec -it jobmanager ./bin/flink cancel <job-id>
```

### Inspecting Kafka topics directly

From the host (requires `kcat` / `kafkacat`):

```bash
# Raw events from the producer
kcat -b localhost:9094 -t test-topic -C -o beginning -e

# Validated events from Flink
kcat -b localhost:9094 -t processed -C -o beginning -e

# Dead-letter records
kcat -b localhost:9094 -t raw-events-dlq -C -o beginning -e
```

---

## Kafka Topics

| Topic | Producer | Consumer | Description |
|-------|----------|----------|-------------|
| `test-topic` | producer-api | Flink job | Raw inbound events (valid + invalid) |
| `processed` | Flink job | parquet-writer | Validated, normalized events |
| `raw-events-dlq` | Flink job | parquet-writer | Failed validation records with error context |

Topics are auto-created by Kafka on first write (default broker behavior).

---

## Message Schemas

### Valid event (producer → `test-topic`)

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "value": 42.17,
  "timestamp": 1723123456.789
}
```

### Processed event (Flink → `processed`)

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "value": 42.17
}
```

### DLQ record (Flink → `raw-events-dlq`)

```json
{
  "raw": "{this is not valid json::",
  "error": "JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
}
```

### Intentional failure modes (burst mode)

| Mode | Example | Flink error |
|------|---------|-------------|
| Invalid JSON | `{this is not valid json::` | `JSONDecodeError` |
| Missing field | `{"event_id": "..."}`  (no `value`) | `KeyError` |
| Wrong type | `{"value": "not-a-number"}` | `ValueError` |

---

## Flink Job Details

**Job name:** `kafka-validate-dlq-pipeline`

| Setting | Value | Rationale |
|---------|-------|-----------|
| Parallelism | 1 | Simplifies local debugging |
| Checkpointing | EXACTLY_ONCE, every 10s | Atomic offset commits |
| State backend | RocksDB (incremental) | Efficient checkpoint storage |
| Checkpoint dir | `file:///checkpoints` | Persisted to host via bind mount |
| Min pause between checkpoints | 5s | Prevents checkpoint storms |
| Checkpoint timeout | 60s | Fail and retry slow checkpoints |
| Starting offsets | earliest | Read all historical data on first run |

The `ValidateAndSplit` ProcessFunction parses each message as JSON, coerces `value` to `float`, and routes failures to the DLQ side output with the original payload and exception details.

---

## Parquet Output

Files are written to `./parquet-output/` with the naming convention:

```
processed_<unix_timestamp>.parquet
raw-events-dlq_<unix_timestamp>.parquet
```

| Setting | Default | Description |
|---------|---------|-------------|
| `BATCH_SIZE` | 100 | Flush after N records per topic |
| `BATCH_TIMEOUT_SEC` | 30 | Flush after N seconds regardless of batch size |

Each flush creates a **new file** (no in-place appends). Schema is inferred by PyArrow from the dict keys present in each batch.

---

## Querying Results

### Using the included notebook

Open `QueryTheLogs.ipynb` in Jupyter and run the cells. It uses DuckDB to query Parquet files with SQL:

```bash
pip install duckdb jupyter
jupyter notebook QueryTheLogs.ipynb
```

### Using DuckDB CLI

```bash
pip install duckdb

duckdb -c "SELECT * FROM read_parquet('./parquet-output/processed_*.parquet') LIMIT 10;"
duckdb -c "SELECT COUNT(*) FROM read_parquet('./parquet-output/raw-events-dlq_*.parquet');"
```

### Using Python (pandas)

```python
import pandas as pd

df = pd.read_parquet("./parquet-output/", filters=[("filename", "like", "processed%")])
print(df.describe())
```

---

## Project Structure

```
StreamingLocally/
├── docker-compose.yml          # Full stack orchestration
├── README.md                   # This file
├── QueryTheLogs.ipynb          # Jupyter notebook for querying Parquet output
├── .gitignore
│
├── producer/                   # FastAPI event producer
│   ├── app.py                  # HTTP API + Kafka producer logic
│   ├── Dockerfile
│   └── requirements.txt
│
├── flink-job/                  # Apache Flink validation pipeline
│   ├── job.py                  # PyFlink streaming job (validate + DLQ)
│   └── Dockerfile
│
├── parquet-writer/             # Kafka → Parquet archival service
│   ├── writer.py               # Consumer loop + batch writer
│   ├── Dockerfile
│   └── requirements.txt
│
├── checkpoints/                # (gitignored) Flink checkpoint state
└── parquet-output/             # (gitignored) Generated Parquet files
```

---

## Configuration Reference

All services communicate over the Docker Compose network using the hostname `kafka` for the broker. Key environment variables and constants:

| Location | Variable | Default | Description |
|----------|----------|---------|-------------|
| `producer/app.py` | `KAFKA_BOOTSTRAP` | `kafka:9092` | Broker address |
| `producer/app.py` | `KAFKA_TOPIC` | `test-topic` | Destination topic |
| `flink-job/job.py` | `KAFKA_BOOTSTRAP` | `kafka:9092` | Broker address |
| `flink-job/job.py` | `CONSUMER_GROUP` | `flink-debug-group` | Flink consumer group |
| `parquet-writer/writer.py` | `BATCH_SIZE` | `100` | Records per flush |
| `parquet-writer/writer.py` | `BATCH_TIMEOUT_SEC` | `30` | Seconds between flushes |
| `docker-compose.yml` | Flink checkpoint interval | `10000ms` | Via `enable_checkpointing` |

To change burst bad-message rate, edit `bad_rate=0.5` in the `/start` endpoint handler in `producer/app.py`.

---

## Troubleshooting

### Kafka connection refused

Wait for Kafka to finish starting (can take 15–30 seconds after `docker compose up`):

```bash
docker compose logs kafka --tail 20
# Look for: "Kafka Server started"
```

### Flink job not processing messages

1. Confirm the job is submitted and running: http://localhost:8081
2. Check TaskManager logs: `docker compose logs taskmanager`
3. Ensure the producer is sending events: `curl -X POST http://localhost:8000/produce`

### No Parquet files appearing

1. Confirm the Flink job is running and writing to `processed` / `raw-events-dlq`
2. Check parquet-writer logs: `docker compose logs parquet-writer`
3. Wait at least 30 seconds (batch timeout) or produce 100+ messages

### Port already in use

```bash
# Check what's using port 8000, 8081, 9092, or 9094
ss -tlnp | grep -E '8000|8081|9092|9094'
```

Change port mappings in `docker-compose.yml` if needed.

### Reset everything

```bash
docker compose down -v          # Stop and remove volumes (deletes Kafka data)
rm -rf checkpoints/ parquet-output/
docker compose up --build -d    # Fresh start
```

---

## Cleanup

```bash
# Stop all services (preserves Kafka data and checkpoints)
docker compose down

# Stop and delete all data volumes
docker compose down -v

# Remove generated output directories
rm -rf checkpoints/ parquet-output/
```

---

## License

This project is provided as a learning demo. Use and modify freely.
