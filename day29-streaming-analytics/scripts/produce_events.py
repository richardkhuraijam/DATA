#!/usr/bin/env python3
"""Produce sample clickstream JSON events to Kafka."""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout"]
PAGES = ["/home", "/search", "/products/1", "/cart", "/checkout"]


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    print("Sending clickstream events to clickstream-events. Press Ctrl+C to stop.")
    try:
        while True:
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": f"user-{random.randint(1, 100)}",
                "event_type": random.choice(EVENT_TYPES),
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "page": random.choice(PAGES),
            }
            producer.send("clickstream-events", event)
            producer.flush()
            print(event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
