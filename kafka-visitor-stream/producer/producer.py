from kafka import KafkaProducer
import json
import uuid
import time
from datetime import datetime


KAFKA_SERVER = "localhost:9094"
TOPIC = "visitor-events"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def create_visitor_event():

    visitor_id = "VIS-" + str(uuid.uuid4())[:8]

    event = {
        "visitor_id": visitor_id,
        "name": "Richard",
        "page": "/home",
        "action": "page_view",
        "timestamp": datetime.now().isoformat()
    }

    return event


print("===================================")
print("   VISITOR KAFKA PRODUCER")
print("===================================")
print("Kafka:", KAFKA_SERVER)
print("Topic:", TOPIC)
print("")


try:

    while True:

        event = create_visitor_event()

        producer.send(
            TOPIC,
            key=event["visitor_id"],
            value=event
        )

        producer.flush()

        print("Visitor Event Sent")
        print(event)
        print("-" * 60)

        time.sleep(3)


except KeyboardInterrupt:

    print("\nProducer stopped.")

finally:

    producer.close()
