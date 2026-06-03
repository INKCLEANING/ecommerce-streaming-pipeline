import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-streaming-raw")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

TOPICS = ["orders.placed", "orders.shipped", "page.views", "orders.dlq"]
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "500"))

events_consumed = Counter("consumer_events_total", "Events consumed from Kafka", ["topic"])
events_written = Counter("consumer_s3_writes_total", "Batches written to S3", ["topic"])
dlq_count = Gauge("consumer_dlq_messages_total", "Messages in dead-letter queue")
s3_write_latency = Histogram("consumer_s3_write_seconds", "S3 write latency in seconds")

s3 = boto3.client("s3", region_name=AWS_REGION)

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "ecommerce-s3-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)
consumer.subscribe(TOPICS)

# topic → list of raw JSON strings buffered before S3 flush
buffers: dict[str, list[str]] = defaultdict(list)
last_flush = time.time()


def s3_key(topic: str, now: datetime) -> str:
    date_path = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
    ts = now.strftime("%Y%m%dT%H%M%S%f")
    return f"{topic}/{date_path}/{ts}.json"


def flush_to_s3(topic: str, records: list[str]):
    if not records:
        return
    now = datetime.now(timezone.utc)
    key = s3_key(topic, now)
    body = "\n".join(records)
    start = time.time()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    s3_write_latency.observe(time.time() - start)
    events_written.labels(topic=topic).inc()
    print(f"[S3] Wrote {len(records)} records → s3://{S3_BUCKET}/{key}")


def flush_all():
    for topic, records in buffers.items():
        if records:
            flush_to_s3(topic, records)
    buffers.clear()


if __name__ == "__main__":
    start_http_server(8001)
    print(f"Consumer started — listening to {TOPICS} on {KAFKA_SERVERS}")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] {msg.error()}")
            else:
                topic = msg.topic()
                value = msg.value().decode("utf-8")
                buffers[topic].append(value)
                events_consumed.labels(topic=topic).inc()

                if topic == "orders.dlq":
                    dlq_count.inc()

                # flush on batch size or time interval
                if (
                    len(buffers[topic]) >= FLUSH_BATCH_SIZE
                    or time.time() - last_flush >= FLUSH_INTERVAL_SECONDS
                ):
                    flush_all()
                    last_flush = time.time()
    finally:
        flush_all()
        consumer.close()
