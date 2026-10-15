"""Kafka helpers: reliable producer (acks=all, idempotent) and topic creation (RF=3, min.insync.replicas=2)."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("kafka_utils")

TOPICS = {"trip_requests": 6, "driver_gps_pings": 6, "weather": 1, "surge_updates": 6}


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")


def create_producer() -> Producer:
    return Producer({"bootstrap.servers": get_bootstrap_servers(), "acks": "all", "enable.idempotence": True,
                     "compression.type": "lz4", "linger.ms": 20})


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Message delivery failed: %s", err)


def send(producer: Producer, topic: str, key: str, event: dict) -> None:
    producer.produce(topic=topic, key=str(key), value=json.dumps(event), callback=delivery_report)
    producer.poll(0)


def create_topics(replication_factor: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": get_bootstrap_servers()})
    brokers = len(admin.list_topics(timeout=10).brokers)
    rf = min(replication_factor, brokers)
    min_isr = "2" if rf >= 3 else "1"
    new = [NewTopic(t, num_partitions=p, replication_factor=rf, config={"min.insync.replicas": min_isr}) for t, p in TOPICS.items()]
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result(); logger.info("created topic %s (partitions=%s RF=%s min.isr=%s)", topic, TOPICS[topic], rf, min_isr)
        except Exception as exc:  # noqa: BLE001
            logger.info("topic %s: %s", topic, exc)


if __name__ == "__main__":
    create_topics()
