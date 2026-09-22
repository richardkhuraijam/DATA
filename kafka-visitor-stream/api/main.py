from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
import json
import uuid
import sqlite3
from datetime import datetime


# ==========================================
# FastAPI
# ==========================================

app = FastAPI(
    title="Kafka Visitor Tracking API",
    description="Real-time visitor tracking using FastAPI and Kafka",
    version="1.0"
)


# ==========================================
# CORS
# ==========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# Kafka Configuration
# ==========================================

KAFKA_SERVER = "localhost:9094"
TOPIC = "visitor-events"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,

    key_serializer=lambda key:
        key.encode("utf-8"),

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ==========================================
# Home
# ==========================================

@app.get("/")
def home():

    return {
        "message": "Kafka Visitor Tracking API is running",
        "kafka": KAFKA_SERVER,
        "topic": TOPIC
    }


# ==========================================
# Visitor Event
# ==========================================

@app.post("/visitor")
def create_visitor(
    page: str = "/home",
    action: str = "page_view",
    visitor_id: str = None
):

    # Create visitor ID if browser didn't provide one

    if visitor_id is None:

        visitor_id = (
            "VIS-" +
            str(uuid.uuid4())[:8]
        )


    # Create event

    event = {

        "visitor_id": visitor_id,

        "name": "Richard",

        "page": page,

        "action": action,

        "timestamp":
            datetime.now().isoformat()
    }


    # Send event to Kafka

    producer.send(
        TOPIC,

        key=visitor_id,

        value=event
    )


    producer.flush()


    # Return the event to browser

    return {

        "message":
            "Visitor event sent to Kafka",

        "event": event
    }


# ==========================================
# Database Connection
# ==========================================

DB_PATH = "visitors.db"


def get_db_connection():

    connection = sqlite3.connect(
        DB_PATH
    )

    connection.row_factory = sqlite3.Row

    return connection


# ==========================================
# Analytics
# ==========================================

@app.get("/analytics")
def analytics():

    connection = get_db_connection()

    cursor = connection.cursor()


    # Total events

    total_events = cursor.execute("""
        SELECT COUNT(*)
        FROM visitors
    """).fetchone()[0]


    # Unique visitors

    unique_visitors = cursor.execute("""
        SELECT COUNT(DISTINCT visitor_id)
        FROM visitors
    """).fetchone()[0]


    # Total pages

    total_pages = cursor.execute("""
        SELECT COUNT(DISTINCT page)
        FROM visitors
    """).fetchone()[0]


    # Top page

    top_page = cursor.execute("""
        SELECT
            page,
            COUNT(*) AS visits

        FROM visitors

        GROUP BY page

        ORDER BY visits DESC

        LIMIT 1
    """).fetchone()


    # Recent visitors

    recent_visitors = cursor.execute("""
        SELECT
            visitor_id,
            page,
            action,
            timestamp

        FROM visitors

        ORDER BY id DESC

        LIMIT 10
    """).fetchall()


    connection.close()


    return {

        "total_events":
            total_events,

        "unique_visitors":
            unique_visitors,

        "total_pages":
            total_pages,

        "top_page":
            dict(top_page)
            if top_page
            else None,

        "recent_visitors":
            [
                dict(row)
                for row in recent_visitors
            ]
}
