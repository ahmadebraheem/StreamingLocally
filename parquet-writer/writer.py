"""
Kafka-to-Parquet Batch Writer
=============================

This module implements a long-running Kafka consumer that subscribes to the
**downstream** topics produced by the Flink validation job and persists their
contents as Apache Parquet files on local disk.

Architecture role
-----------------
    Kafka "processed"       -->  Parquet writer  -->  ./parquet-output/processed_*.parquet
    Kafka "raw-events-dlq"  -->  Parquet writer  -->  ./parquet-output/raw-events-dlq_*.parquet

Batching strategy
-----------------
Records are accumulated in per-topic in-memory buffers. A flush (Parquet file
write) is triggered when **either**:

    - A single topic buffer reaches BATCH_SIZE records, OR
    - BATCH_TIMEOUT_SEC seconds have elapsed since the last flush

This hybrid approach balances file size (fewer small files) against latency
(data is not held indefinitely waiting for a full batch).

Deployment
----------
Runs as the ``parquet-writer`` service in docker-compose.yml. The output
directory is bind-mounted to ``./parquet-output`` on the host for easy
inspection with DuckDB, pandas, or the included QueryTheLogs.ipynb notebook.
"""

import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Kafka bootstrap address inside the Docker Compose network
KAFKA_BOOTSTRAP = "kafka:9092"

# Topics to consume — both the happy-path and dead-letter outputs from Flink
TOPICS = ["processed", "raw-events-dlq"]

# Directory where Parquet files are written (bind-mounted in docker-compose)
OUTPUT_DIR = Path("./parquet-output")

# Flush to disk after this many records per topic
BATCH_SIZE = 100

# Flush to disk after this many seconds even if BATCH_SIZE is not reached
BATCH_TIMEOUT_SEC = 30

# Consumer group — Kafka uses this to track committed offsets across restarts
CONSUMER_GROUP = "parquet-writer"

# Ensure the output directory exists before we attempt any writes
OUTPUT_DIR.mkdir(exist_ok=True)


def write_batch(topic: str, records: list[dict]) -> None:
    """
    Serialize a batch of records to a single Parquet file.

    Parameters
    ----------
    topic : str
        Kafka topic name; used as a filename prefix so outputs from different
        topics are easy to distinguish on disk.
    records : list[dict]
        Python dicts parsed from Kafka message values. PyArrow infers the
        schema from the dict keys present in the batch.

    Notes
    -----
    Filename format: ``{topic}_{unix_timestamp}.parquet``
    Each flush creates a new file; there is no append-in-place behavior.
    """
    if not records:
        return

    # Convert list of dicts to a columnar Arrow Table (Parquet's native format)
    table = pa.Table.from_pylist(records)

    # Unix timestamp in filename avoids collisions across rapid flushes
    filename = OUTPUT_DIR / f"{topic}_{int(time.time())}.parquet"
    pq.write_table(table, filename)
    print(f"wrote {len(records)} records to {filename}")


def run() -> None:
    """
    Main consumer loop: read from Kafka, buffer, and flush to Parquet.

    The loop runs indefinitely until the process is killed (e.g. container
    stop). On restart, ``auto_offset_reset="earliest"`` combined with the
    consumer group ensures either:
      - Resume from last committed offset (if group state exists), or
      - Read from the beginning of each topic (first run).

    Error handling
    --------------
    Messages that fail JSON parsing are wrapped as ``{"raw": "<string>"}``
    rather than crashing the consumer — this guards against unexpected binary
    payloads landing on the DLQ topic.
    """
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        # On first join with no committed offset, start from topic beginning
        auto_offset_reset="earliest",
        # Decode message values from bytes to UTF-8 strings
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    # Per-topic in-memory buffers; keys mirror TOPICS list
    buffers: dict[str, list[dict]] = {topic: [] for topic in TOPICS}

    # Timestamp of the most recent flush across all topics
    last_flush = time.time()

    # Blocking iterator — yields one ConsumerRecord per Kafka message
    for message in consumer:
        try:
            # Attempt to parse the message value as JSON
            record = json.loads(message.value)
        except json.JSONDecodeError:
            # Fallback: store the raw string so no data is silently dropped
            record = {"raw": message.value}

        # Append to the buffer for the topic this message arrived on
        buffers[message.topic].append(record)

        # Decide whether to flush: per-topic batch full OR global timeout elapsed
        should_flush = (
            len(buffers[message.topic]) >= BATCH_SIZE
            or time.time() - last_flush >= BATCH_TIMEOUT_SEC
        )

        if should_flush:
            # Write all topic buffers and reset them (even empty ones are no-ops)
            for topic, records in buffers.items():
                write_batch(topic, records)
                buffers[topic] = []
            last_flush = time.time()


if __name__ == "__main__":
    run()
