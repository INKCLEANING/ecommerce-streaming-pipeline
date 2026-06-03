import json
import os
import random
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer
from faker import Faker
from prometheus_client import Counter, start_http_server
from pydantic import ValidationError

from schemas import OrderPlaced, OrderShipped, PageView

fake = Faker()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "100"))

TOPIC_ORDERS = "orders.placed"
TOPIC_SHIPPED = "orders.shipped"
TOPIC_PAGEVIEWS = "page.views"
TOPIC_DLQ = "orders.dlq"

events_produced = Counter("producer_events_total", "Total events produced", ["topic"])
events_failed = Counter("producer_events_failed_total", "Events that failed validation", ["event_type"])

producer = Producer({"bootstrap.servers": KAFKA_SERVERS})


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")


def serialize(obj) -> bytes:
    return obj.model_dump_json().encode("utf-8")


def make_order_placed() -> OrderPlaced:
    return OrderPlaced(
        customer_id=f"cust_{fake.random_int(1, 5000)}",
        product_id=f"prod_{fake.random_int(1, 500)}",
        category=random.choice(["electronics", "clothing", "home", "sports", "books", "beauty"]),
        quantity=random.randint(1, 10),
        price=round(random.uniform(5.0, 999.99), 2),
    )


def make_order_shipped() -> OrderShipped:
    return OrderShipped(
        order_id=f"order_{fake.random_int(1, 100000)}",
        carrier=random.choice(["UPS", "FedEx", "USPS", "DHL"]),
        tracking_number=fake.bothify("??##########"),
        estimated_delivery=datetime.utcnow() + timedelta(days=random.randint(1, 7)),
    )


def make_page_view() -> PageView:
    product_id = f"prod_{fake.random_int(1, 500)}"
    return PageView(
        session_id=fake.uuid4(),
        customer_id=f"cust_{fake.random_int(1, 5000)}",
        product_id=product_id,
        page_url=f"/products/{product_id}",
        device_type=random.choice(["desktop", "mobile", "tablet"]),
    )


EVENT_FACTORIES = [
    (TOPIC_ORDERS, make_order_placed, "order_placed"),
    (TOPIC_SHIPPED, make_order_shipped, "order_shipped"),
    (TOPIC_PAGEVIEWS, make_page_view, "page_view"),
]

# page views are most frequent
WEIGHTS = [0.25, 0.15, 0.60]


def produce_event():
    topic, factory, event_type = random.choices(EVENT_FACTORIES, weights=WEIGHTS, k=1)[0]
    try:
        event = factory()
        producer.produce(topic, value=serialize(event), callback=delivery_report)
        events_produced.labels(topic=topic).inc()
    except ValidationError as exc:
        events_failed.labels(event_type=event_type).inc()
        producer.produce(
            TOPIC_DLQ,
            value=json.dumps({"event_type": event_type, "error": str(exc)}).encode(),
        )


if __name__ == "__main__":
    start_http_server(8000)
    print(f"Producer started — targeting {EVENTS_PER_SECOND} events/s → {KAFKA_SERVERS}")
    interval = 1.0 / EVENTS_PER_SECOND
    while True:
        produce_event()
        producer.poll(0)
        time.sleep(interval)
