# Day 29 — Real-Time Streaming Analytics with Kafka and Spark

## Goal
Consume clickstream events from Kafka, calculate rolling metrics with Spark Structured Streaming, and print per-window activity counts.

## Stack
- Java 17
- Scala 2.12.18
- Apache Spark 3.5.3
- Apache Kafka (local broker at `localhost:9092`)
- sbt 1.10.0

## Project structure
```text
day29-streaming-analytics/
├── build.sbt
├── project/build.properties
├── src/main/scala/StreamingAnalytics.scala
└── scripts/produce_events.py
```

## 1. Prerequisites
Confirm Java, Scala, sbt, and Kafka are installed:
```bash
java -version
scala -version
sbt --version
```
Kafka must be running and reachable at `localhost:9092`.

## 2. Create the topic
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic clickstream-events --partitions 1 --replication-factor 1
```

If your Kafka installation uses a different path, adjust the command to your installation.

## 3. Run the Spark streaming application
From the repository root:
```bash
cd day29-streaming-analytics
sbt run
```

The application subscribes to `clickstream-events`, parses JSON events, filters malformed records, groups events into 1-minute event-time windows, and prints counts by event type.

## 4. Start the sample producer
In a second terminal, from the repository root:
```bash
python3 -m pip install kafka-python
python3 day29-streaming-analytics/scripts/produce_events.py
```

The producer sends sample page-view, search, add-to-cart, and checkout events every second.

## Example event
```json
{"event_id":"evt-1001","user_id":"user-42","event_type":"page_view","event_time":"2026-10-18T10:00:00Z","page":"/home"}
```

## Expected behavior
After a window closes, Spark prints counts grouped by window start/end and event type. Output depends on the events produced and event-time progress.

## Troubleshooting
- **Connection refused:** confirm Kafka is running and listening on `localhost:9092`.
- **No output immediately:** event-time windows emit after the window watermark advances; keep the producer running for several minutes.
- **Python producer import error:** install `kafka-python` in the Python environment used to run the script.
- **Java module access errors:** the build includes common Java 17 `--add-opens` options.

## Learning checklist
- [ ] Create a Kafka topic.
- [ ] Start Spark Structured Streaming.
- [ ] Produce events from a second terminal.
- [ ] Explain event time, windows, watermarking, and aggregation.
