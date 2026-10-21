#!/usr/bin/env python3
"""Send sample transaction JSON events to Kafka for the Day 28 demo."""

import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

merchants = ["Grocery Store", "Fuel Station", "Electronics Store", "Online Shop"]
countries = ["IN", "IN", "IN", "US", "SG"]

print(f"Sending sample transactions to {BOOTSTRAP_SERVERS}, topic={TOPIC}. Ctrl+C to stop.")
counter = 1000

try:
    while True:
        counter += 1
        suspicious = random.random() < 0.3
        amount = round(random.uniform(1000, 25000), 2)
        international = False

        if suspicious and random.random() < 0.5:
            amount = round(random.uniform(100000, 180000), 2)
        elif suspicious:
            international = True

        event = {
            "transaction_id": f"TXN-{counter}",
            "account_id": f"ACC-{random.randint(100, 105)}",
            "amount": amount,
            "merchant": random.choice(merchants),
            "country": random.choice(countries) if international else "IN",
            "event_time": datetime.now().replace(microsecond=0).isoformat(),
            "is_international": international,
        }

        producer.send(TOPIC, event)
        producer.flush()
        print("Sent:", json.dumps(event))
        time.sleep(2)
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.close()
