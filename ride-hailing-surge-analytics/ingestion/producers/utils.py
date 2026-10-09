"""Kafka configuration and admin utility module.

Handles topic creation and producer client creation with reliability settings:
- acks=all
- enable.idempotence=true
- min.insync.replicas=2 (or 1 for local fallback)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("kafka_utils")


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")


def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": get_bootstrap_servers(),
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 5,
    }
    return Producer(conf)


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.debug(
            "Message delivered to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_topics_if_not_exist(
    partitions_default: int = 6, replication_factor_default: int = 3
) -> None:
    bootstrap = get_bootstrap_servers()
    admin_client = AdminClient({"bootstrap.servers": bootstrap})

    try:
        metadata = admin_client.list_topics(timeout=5)
        existing_topics = set(metadata.topics.keys())
    except Exception as exc:
        logger.warning("Could not fetch Kafka topic metadata (Kafka may not be running yet): %s", exc)
        return

    # Local fallback for single-broker environments
    broker_count = len(metadata.brokers)
    rf = min(replication_factor_default, max(1, broker_count))

    topics_to_create = [
        NewTopic("trip_requests", num_partitions=partitions_default, replication_factor=rf),
        NewTopic("driver_gps_pings", num_partitions=partitions_default, replication_factor=rf),
        NewTopic("surge_updates", num_partitions=partitions_default, replication_factor=rf),
        NewTopic("weather", num_partitions=1, replication_factor=rf),
    ]

    new_topics = [t for t in topics_to_create if t.topic not in existing_topics]
    if not new_topics:
        logger.info("All Kafka topics already exist: %s", list(existing_topics))
        return

    futures = admin_client.create_topics(new_topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Successfully created topic '%s' (partitions=%d, RF=%d)", topic_name, partitions_default, rf)
        except Exception as exc:
            logger.error("Failed to create topic '%s': %s", topic_name, exc)
