import asyncio
import json
import random
import time
import uuid

from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

producer: AIOKafkaProducer | None = None
running = False
burst_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(lifespan=lifespan)


def make_message() -> bytes:
    payload = {
        "event_id": str(uuid.uuid4()),
        "value": round(random.uniform(0, 100), 2),
        "timestamp": time.time(),
    }
    return json.dumps(payload).encode()

def make_bad_message() -> bytes:
    kind = random.choice(["not_json", "missing_field", "bad_type"])
    if kind == "not_json":
        return b"{this is not valid json::"
    if kind == "missing_field":
        return json.dumps({"event_id": str(uuid.uuid4())}).encode()  # no 'value'
    return json.dumps({
        "event_id": str(uuid.uuid4()),
        "value": "not-a-number",
        "timestamp": time.time(),
    }).encode()


async def burst_loop(rate_per_sec: float, bad_rate: float = 0.15):
    global running
    delay = 1.0 / rate_per_sec
    while running:
        message = make_bad_message() if random.random() < bad_rate else make_message()
        await producer.send_and_wait("test-topic", message)
        await asyncio.sleep(delay)


@app.post("/start")
async def start(rate_per_sec: float = 10):
    global running, burst_task
    if running:
        return {"status": "already running"}
    running = True
    burst_task = asyncio.create_task(burst_loop(rate_per_sec, bad_rate=0.5))
    return {"status": "started", "rate_per_sec": rate_per_sec}


@app.post("/stop")
async def stop():
    global running
    running = False
    return {"status": "stopped"}


@app.post("/produce")
async def produce():
    await producer.send_and_wait("test-topic", make_message())
    return {"status": "sent"}