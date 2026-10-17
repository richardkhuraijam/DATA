from kafka import KafkaConsumer
import json
import sqlite3


KAFKA_SERVER = "localhost:9094"
TOPIC = "visitor-events"


# --------------------------------
# Database Connection
# --------------------------------

connection = sqlite3.connect("visitors.db")

cursor = connection.cursor()


# --------------------------------
# Create Visitors Table
# --------------------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS visitors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    visitor_id TEXT NOT NULL,
    name TEXT,
    page TEXT,
    action TEXT,
    timestamp TEXT
)
""")

connection.commit()


# --------------------------------
# Kafka Consumer
# --------------------------------

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="visitor-database-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,

    key_deserializer=lambda key:
        key.decode("utf-8") if key else None,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


print("===================================")
print("   VISITOR DATABASE CONSUMER")
print("===================================")
print("Kafka:", KAFKA_SERVER)
print("Topic:", TOPIC)
print("Database: visitors.db")
print("")
print("Waiting for visitor events...\n")


try:

    for message in consumer:

        visitor = message.value

        print("Visitor Event Received")
        print("-----------------------------------")
        print("Visitor ID :", visitor["visitor_id"])
        print("Name       :", visitor["name"])
        print("Page       :", visitor["page"])
        print("Action     :", visitor["action"])
        print("Timestamp  :", visitor["timestamp"])

        # ----------------------------
        # Save visitor to database
        # ----------------------------

        cursor.execute("""
        INSERT INTO visitors
        (
            visitor_id,
            name,
            page,
            action,
            timestamp
        )
        VALUES (?, ?, ?, ?, ?)
        """, (
            visitor["visitor_id"],
            visitor["name"],
            visitor["page"],
            visitor["action"],
            visitor["timestamp"]
        ))

        connection.commit()

        print("✓ Visitor saved to database")
        print("-----------------------------------\n")


except KeyboardInterrupt:

    print("\nConsumer stopped.")


finally:

    consumer.close()
    connection.close()
