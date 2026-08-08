import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "kafka:9092"  
TOPICS = ["processed", "raw-events-dlq"]
OUTPUT_DIR = Path("./parquet-output")
BATCH_SIZE = 100          # write a file every N messages...
BATCH_TIMEOUT_SEC = 30    # ...or every N seconds, whichever comes first

OUTPUT_DIR.mkdir(exist_ok=True)


def write_batch(topic: str, records: list[dict]):
    if not records:
        return
    table = pa.Table.from_pylist(records)
    filename = OUTPUT_DIR / f"{topic}_{int(time.time())}.parquet"
    pq.write_table(table, filename)
    print(f"wrote {len(records)} records to {filename}")


def run():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="parquet-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    buffers = {topic: [] for topic in TOPICS}
    last_flush = time.time()

    for message in consumer:
        try:
            record = json.loads(message.value)
        except json.JSONDecodeError:
            record = {"raw": message.value}

        buffers[message.topic].append(record)

        should_flush = (
            len(buffers[message.topic]) >= BATCH_SIZE
            or time.time() - last_flush >= BATCH_TIMEOUT_SEC
        )
        if should_flush:
            for topic, records in buffers.items():
                write_batch(topic, records)
                buffers[topic] = []
            last_flush = time.time()


if __name__ == "__main__":
    run()