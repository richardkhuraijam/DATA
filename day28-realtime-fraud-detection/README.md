# Day 28 — Real-Time Fraud Detection with Kafka and Spark Structured Streaming

A beginner-friendly Scala 2.12 + Apache Spark 3.5.3 project that consumes transaction events from Kafka and prints rule-based fraud alerts.

> Educational demo only. The rules are intentionally simple and are not suitable for real financial decisions.

## What you'll learn
- Read a Kafka topic with Spark Structured Streaming.
- Parse JSON events into a typed Spark schema.
- Apply basic fraud rules to a live stream.
- Print suspicious transactions to the console.
- Run a small Kafka producer to generate sample events.

## Project layout
```text
day28-realtime-fraud-detection/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── scripts/
│   └── produce_transactions.py
└── src/main/scala/
    └── RealtimeFraudDetection.scala
```

## Requirements
- Java 17
- Scala 2.12 (SBT downloads the project dependencies)
- Apache Spark 3.5.3 dependencies via SBT
- Kafka reachable at `localhost:9092`
- Python 3 and `kafka-python` for the sample producer

This project assumes Kafka's advertised listener is available at `localhost:9092`. Adjust the `bootstrap.servers` value in both the Scala file and producer if your Kafka listener uses another port.

## 1. Start Kafka
Start your local Kafka broker using the setup you already use. Confirm the broker is reachable before continuing.

Create the topic:
```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 1 --replication-factor 1
```

Check it:
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 2. Run the Spark streaming app
From this project directory:
```bash
sbt run
```

The first run may take time because SBT downloads dependencies. Keep this terminal open.

The application reads JSON messages from the `transactions` topic. It flags a transaction when:
- `amount >= 100000`, or
- `is_international` is `true`.

Alerts are printed to the console. Normal transactions are ignored.

## 3. Start the sample producer
In a second terminal, enter this project directory and install the producer dependency:
```bash
python3 -m pip install kafka-python
```

Then run:
```bash
python3 scripts/produce_transactions.py
```

The producer sends a mixture of normal and suspicious sample transactions every two seconds. Press `Ctrl+C` to stop.

## 4. Example event
```json
{
  "transaction_id": "TXN-1001",
  "account_id": "ACC-101",
  "amount": 150000.0,
  "merchant": "Electronics Store",
  "country": "IN",
  "event_time": "2026-10-18T12:00:00",
  "is_international": false
}
```

## 5. Stop the app
Press `Ctrl+C` in the Spark terminal. Spark may create checkpoint data in a temporary directory under `/tmp`.

## Troubleshooting
- **Connection refused:** confirm Kafka is running and that `localhost:9092` is the correct listener.
- **No alerts:** keep both the Spark app and producer running; most sample transactions are intentionally normal.
- **Missing Kafka source:** run the app through `sbt run`; the Kafka connector dependency is declared in `build.sbt`.
- **Java module access error:** this project includes the required Java 17 `--add-opens` options in `build.sbt`.
- **Dependency download error:** check your internet connection and retry `sbt run`.

## Learning tasks
1. Change the high-value threshold from `100000` to `50000`.
2. Add another rule, such as flagging an account when the country is not `IN`.
3. Save alerts to a file or database.
4. Add a rolling count of transactions per account as a separate exercise.

## GitHub
From the root of your `scala-spark-30-days` repository:
```bash
git add day28-realtime-fraud-detection
git diff --cached --check
git commit -m "Add Day 28 real-time fraud detection"
git push origin HEAD:main
```
Before pushing, make sure `git status` shows only the intended Day 28 project staged. Do not use `git add .` if generated files are present.
