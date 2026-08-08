"""
Flink Kafka Validation & DLQ Pipeline
======================================

This module defines an Apache Flink streaming job that:

    1. Consumes raw JSON events from the Kafka topic ``test-topic``
    2. Validates and normalizes each record (parses JSON, coerces ``value`` to float)
    3. Routes valid records to the ``processed`` Kafka topic
    4. Routes invalid records to the ``raw-events-dlq`` dead-letter topic

Architecture role
-----------------
    Kafka "test-topic"  -->  Flink ValidateAndSplit  -->  Kafka "processed"
                                    |
                                    +-->  Kafka "raw-events-dlq"

The job uses Flink checkpointing (EXACTLY_ONCE) with RocksDB state backend
so that offset commits and side-output routing are consistent across failures.

Deployment
----------
Submit this job to the Flink cluster defined in docker-compose.yml::

    docker exec -it jobmanager ./bin/flink run \\
        -py /opt/flink/usrlib/job.py

The job JAR (flink-sql-connector-kafka) is baked into the Docker image;
see flink-job/Dockerfile for details.
"""

import json

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.output_tag import OutputTag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Kafka bootstrap address inside the Docker Compose network
KAFKA_BOOTSTRAP = "kafka:9092"

# Source topic: raw events from the producer API
SOURCE_TOPIC = "test-topic"

# Sink topic for successfully validated and normalized events
PROCESSED_TOPIC = "processed"

# Sink topic for records that failed validation (dead-letter queue)
DLQ_TOPIC = "raw-events-dlq"

# Consumer group ID — Flink uses this to track read offsets in Kafka
CONSUMER_GROUP = "flink-debug-group"

# Side-output tag for routing failed records out of the main stream.
# OutputTag is Flink's mechanism for emitting secondary streams from a
# single ProcessFunction without duplicating the source read.
DLQ_TAG = OutputTag("dlq", Types.STRING())


class ValidateAndSplit(ProcessFunction):
    """
    Per-record validation and normalization operator.

  Inherits from Flink's ProcessFunction so each incoming Kafka message value
  (a UTF-8 JSON string) is inspected individually. Valid records are emitted
  on the main output; invalid records are emitted to the DLQ side output.

  Validation rules
  ----------------
  - Payload must be valid JSON (json.loads must succeed)
  - Payload must contain a ``value`` key
  - ``value`` must be coercible to float

  Output schema (main stream)
  ---------------------------
  {"event_id": "<string>", "value": <float>}

  Output schema (DLQ side output)
  -------------------------------
  {"raw": "<original string>", "error": "<ExceptionType>: <message>"}
    """

    def process_element(self, value, ctx):
        """
        Process a single incoming record from the Kafka source.

        Parameters
        ----------
        value : str
            The Kafka message value deserialized as a UTF-8 string.
        ctx : ProcessFunction.Context
            Flink runtime context; ``ctx.output(tag, data)`` emits to a
            side output stream.

        Yields
        ------
        str
            Normalized JSON string on the main output for valid records.
        tuple[OutputTag, str]
            (DLQ_TAG, error JSON) for records that fail validation.
        """
        try:
            # Parse the raw JSON string into a Python dict
            obj = json.loads(value)

            # Coerce "value" to float — raises KeyError if missing,
            # ValueError/TypeError if not numeric
            reading = float(obj["value"])

            # Re-serialize a slim normalized record (drops timestamp, etc.)
            yield json.dumps({"event_id": obj["event_id"], "value": reading})

        except Exception as e:
            # Any validation failure goes to the DLQ side output with context
            yield DLQ_TAG, json.dumps({
                "raw": value,
                "error": f"{type(e).__name__}: {e}",
            })


def make_sink(topic: str) -> KafkaSink:
    """
    Factory for a KafkaSink that writes string values to the given topic.

    Parameters
    ----------
    topic : str
        Destination Kafka topic name.

    Returns
    -------
    KafkaSink
        Configured sink ready to attach to a DataStream via ``.sink_to()``.
    """
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main():
    """
    Build and execute the Flink streaming pipeline.

    Pipeline stages
    ---------------
    1. Create execution environment with parallelism 1 (demo simplicity)
    2. Enable EXACTLY_ONCE checkpointing every 10 seconds
    3. Read from Kafka source (earliest offsets, string deserializer)
    4. Apply ValidateAndSplit ProcessFunction
    5. Sink main stream -> "processed", side output -> "raw-events-dlq"
  6. Block until the job completes (runs indefinitely in practice)
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    # Parallelism 1 keeps the demo easy to reason about on a single TaskManager
    env.set_parallelism(1)

    # Checkpoint every 10 seconds with EXACTLY_ONCE semantics.
    # This ensures Kafka offsets are committed atomically with processed state.
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)

    ckpt = env.get_checkpoint_config()
    # Prevent checkpoint storms: wait at least 5s between consecutive checkpoints
    ckpt.set_min_pause_between_checkpoints(5000)
    # Fail the checkpoint (and retry) if it does not complete within 60 seconds
    ckpt.set_checkpoint_timeout(60000)

    # -----------------------------------------------------------------------
    # Source: read raw events from Kafka
    # -----------------------------------------------------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        # Start from the beginning of the topic on first run (no committed offset)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # WatermarkStrategy.no_watermarks() — we are not doing event-time windows
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source",
    )

    # -----------------------------------------------------------------------
    # Transform: validate and split into main + DLQ side output
    # -----------------------------------------------------------------------
    main_stream = raw_stream.process(
        ValidateAndSplit(),
        output_type=Types.STRING(),
    )

    # Extract the side output stream tagged with DLQ_TAG
    dlq_stream = main_stream.get_side_output(DLQ_TAG)

    # -----------------------------------------------------------------------
    # Sinks: write validated and failed records to separate Kafka topics
    # -----------------------------------------------------------------------
    main_stream.sink_to(make_sink(PROCESSED_TOPIC))
    dlq_stream.sink_to(make_sink(DLQ_TOPIC))

    # Submit the job to the Flink cluster (JobManager)
    env.execute("kafka-validate-dlq-pipeline")


if __name__ == "__main__":
    main()
