"""
Kafka Event Producer API
========================

This module implements a FastAPI HTTP service that generates synthetic JSON
events and publishes them to a Kafka topic. It is the **entry point** of the
StreamingLocally pipeline: all downstream processing (Flink validation, DLQ
routing, Parquet archival) begins with messages produced here.

Architecture role
-----------------
    HTTP Client  -->  FastAPI (this service)  -->  Kafka topic "test-topic"
                                                          |
                                                          v
                                                   Flink job (validation)

Endpoints
---------
    POST /start   Begin a continuous burst of messages at a configurable rate.
    POST /stop    Halt the burst loop (in-flight messages may still complete).
    POST /produce Send a single valid message on demand.

Message schema (valid events)
-----------------------------
    {
        "event_id": "<uuid4 string>",
        "value":    <float 0-100>,
        "timestamp": <unix epoch seconds>
    }

The burst loop intentionally injects malformed messages (~50% during /start) so
the Flink validation job can demonstrate dead-letter-queue (DLQ) handling.
"""

import asyncio
import json
import random
import time
import uuid

from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# The Kafka producer is created once at startup and shared across all requests.
# Using a module-level singleton avoids reconnecting to the broker per request.
producer: AIOKafkaProducer | None = None

# Flag that controls the background burst loop. Set True by /start, False by /stop.
running = False

# Reference to the asyncio Task running burst_loop; used to avoid spawning
# duplicate burst tasks if /start is called while already running.
burst_task: asyncio.Task | None = None

# Kafka topic that all messages are written to. Downstream Flink job consumes
# from this same topic name.
KAFKA_TOPIC = "test-topic"

# Bootstrap address for the Kafka broker inside the Docker Compose network.
# "kafka" resolves to the kafka service defined in docker-compose.yml.
KAFKA_BOOTSTRAP = "kafka:9092"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan hook: manage the Kafka producer connection lifecycle.

    Runs once when the Uvicorn worker starts (before accepting requests) and
    once again on shutdown. Ensures the producer is connected before any
    endpoint can call send_and_wait(), and cleanly closes the connection when
    the container stops.
    """
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(
    lifespan=lifespan,
    title="StreamingLocally Producer",
    description="Synthetic event generator for the on-prem streaming demo.",
)


def make_message() -> bytes:
    """
    Build a single **valid** event payload serialized as UTF-8 JSON bytes.

    Returns
    -------
    bytes
        JSON-encoded event ready to be sent as a Kafka message value.

    Schema
    ------
    event_id  : unique identifier (UUID4) for idempotency / dedup downstream
    value     : random float in [0, 100], rounded to 2 decimal places
    timestamp : wall-clock time when the message was created (epoch seconds)
    """
    payload = {
        "event_id": str(uuid.uuid4()),
        "value": round(random.uniform(0, 100), 2),
        "timestamp": time.time(),
    }
    return json.dumps(payload).encode()


def make_bad_message() -> bytes:
    """
    Build a single **invalid** event payload for DLQ testing.

    Randomly selects one of three failure modes that the Flink ValidateAndSplit
    ProcessFunction is designed to catch:

    1. not_json       - raw bytes that are not valid JSON at all
    2. missing_field  - valid JSON but missing the required "value" key
    3. bad_type       - valid JSON but "value" is a string instead of a number

    Returns
    -------
    bytes
        Malformed payload bytes; Flink will route these to raw-events-dlq.
    """
    kind = random.choice(["not_json", "missing_field", "bad_type"])

    if kind == "not_json":
        # Completely invalid JSON — json.loads() in Flink will raise JSONDecodeError
        return b"{this is not valid json::"

    if kind == "missing_field":
        # Valid JSON structure but missing the "value" field Flink expects
        return json.dumps({"event_id": str(uuid.uuid4())}).encode()

    # "value" present but wrong type — float() conversion will fail in Flink
    return json.dumps({
        "event_id": str(uuid.uuid4()),
        "value": "not-a-number",
        "timestamp": time.time(),
    }).encode()


async def burst_loop(rate_per_sec: float, bad_rate: float = 0.15):
    """
    Background coroutine that publishes messages continuously until `running`
    is set to False.

    Parameters
    ----------
    rate_per_sec : float
        Target throughput in messages per second. The loop sleeps
        ``1.0 / rate_per_sec`` seconds between sends to approximate this rate.
    bad_rate : float, optional
        Probability [0, 1] that each message will be malformed. Defaults to
        0.15 for organic error injection; /start overrides this to 0.5 so
        half of burst traffic exercises the DLQ path.

    Notes
    -----
    Uses ``send_and_wait`` rather than fire-and-forget ``send`` so that
    backpressure from a slow broker surfaces as await latency rather than
    silently dropping messages in an internal buffer.
    """
    global running
    delay = 1.0 / rate_per_sec

    while running:
        # Flip a coin: produce a bad message with probability `bad_rate`
        message = make_bad_message() if random.random() < bad_rate else make_message()
        await producer.send_and_wait(KAFKA_TOPIC, message)
        await asyncio.sleep(delay)


@app.post("/start")
async def start(rate_per_sec: float = 10):
    """
    Start the continuous message burst loop.

    Query parameters
    ----------------
    rate_per_sec : float, default 10
        How many messages to attempt per second.

    Returns
    -------
    dict
        Status payload. If already running, returns early without spawning
        a second task.

    Side effects
    ------------
    Spawns ``burst_loop`` as an asyncio Task with bad_rate=0.5 so that
    roughly half of burst traffic is intentionally invalid for DLQ demo.
    """
    global running, burst_task
    if running:
        return {"status": "already running"}

    running = True
    # Elevated bad_rate during burst mode makes DLQ behavior easy to observe
    burst_task = asyncio.create_task(burst_loop(rate_per_sec, bad_rate=0.5))
    return {"status": "started", "rate_per_sec": rate_per_sec}


@app.post("/stop")
async def stop():
    """
    Stop the continuous message burst loop.

    Sets ``running = False``; the loop will exit on its next iteration.
    Does not cancel the asyncio Task abruptly — any in-flight send_and_wait
    call will complete before the loop terminates.

    Returns
    -------
    dict
        ``{"status": "stopped"}``
    """
    global running
    running = False
    return {"status": "stopped"}


@app.post("/produce")
async def produce():
    """
    Produce a single valid message immediately (one-shot, no burst loop).

    Useful for manual testing without starting the continuous burst. Always
    sends a well-formed message via ``make_message()``.

    Returns
    -------
    dict
        ``{"status": "sent"}`` after the message is acknowledged by Kafka.
    """
    await producer.send_and_wait(KAFKA_TOPIC, make_message())
    return {"status": "sent"}
