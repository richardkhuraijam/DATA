# Day 30 — Scala + Spark Capstone: End-to-End Transaction Analytics

## Goal
Build a local end-to-end batch pipeline using Scala and Spark: read CSV transactions, validate records, calculate business metrics, and write clean data plus summary outputs.

This capstone is intentionally local-first; Kafka is not required. It is a practical final project tying together data ingestion, data quality, transformations, aggregations, and reproducible outputs.

## Stack
- Java 17
- Scala 2.12.18
- Apache Spark 3.5.3
- sbt 1.10.0

## Project structure
```text
day30-capstone-pipeline/
├── build.sbt
├── project/build.properties
├── data/transactions.csv
├── src/main/scala/TransactionAnalytics.scala
└── output/   # created at runtime; do not commit generated output
```

## 1. Run the project
From the repository root:
```bash
cd day30-capstone-pipeline
sbt run
```

The app reads `data/transactions.csv` relative to the project directory, checks required fields, removes invalid rows, derives a transaction date, and creates:
- `output/clean_transactions/` — validated transaction rows in Parquet
- `output/daily_summary/` — transaction count and total amount by date
- `output/category_summary/` — transaction count and total amount by category

## 2. Example input
The included CSV is a small teaching dataset with normal and intentionally invalid records so you can observe data-quality handling.

Columns:
- `transaction_id`
- `customer_id`
- `transaction_timestamp`
- `amount`
- `category`
- `status`

The pipeline treats rows as invalid if the transaction ID/customer ID/category is missing, timestamp or amount cannot be parsed, or amount is negative.

## 3. Inspect output
After `sbt run` completes:
```bash
find output -maxdepth 2 -type f | head
```
Spark writes Parquet files and metadata beneath each output directory. The exact filenames depend on Spark.

## 4. Learning checklist
- [ ] Run the end-to-end pipeline.
- [ ] Explain the input schema and validation rules.
- [ ] Compare raw and clean row counts.
- [ ] Explain the daily and category aggregations.
- [ ] Identify how you would productionize the pipeline (scheduler, data lake, monitoring, tests).
