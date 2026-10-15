# What was wrong with the first version (and what changed)

Reviewed by running the code, not just reading it. "Proof" = something reproduced, not an opinion.

## A. Results that were invented (most serious - do not present these)
| Where | Problem | Now |
|---|---|---|
| `tests/reconciliation.py` | Counts were hard-coded ("Simulated pipeline counts"); KPIs hard-coded; always printed PASSED | Real reconciliation: Kafka offsets vs raw Bronze vs Silver, plus every engine window re-computed from raw events |
| `lakehouse/athena_benchmark.py` | "Simulated" 450 MB vs 72 MB -> 84% | Real `DataScannedInBytes` from Athena for 5 queries |
| `tests/drills/broker_kill.py` | `produced_count = 1000; consumed_count = 1000` | Actually stops a broker, counts acked produces, offset delta and unique consumed IDs |
| `tests/drills/spark_restart.py` | `expected = 500; recovered = 500` | SIGKILLs the job, restarts on the same checkpoint, verifies batch continuity and row counts |
| `genai/tools.py` | Hard-coded zones, utilisation 0.81, ETA 3.9, revenue, R2 0.78 | Reads HBase / Bronze / forecast / trips; returns `{"error": ...}` when data is missing |
| `genai/assistant_app.py` | Keyword matching + canned text (not an LLM) | LLM tool-calling (Anthropic SDK) over the real tools |
| `ml/features.py`, `ml/train.py` | Random numbers where target = formula of features -> "R2 = 0.9966" | Features from real trip history, time-based split, honest R2 |
| `index.html`, `app.js` | Slide deck stating 84%, 3.9 min, 81%, R2 0.9966 as achieved | Removed from the project |

## B. The streaming engine could not run (proof: `Distinct aggregations are not supported on streaming DataFrames`)
| Problem | Fix |
|---|---|
| `countDistinct` is illegal on streams | `approx_count_distinct` |
| Stream-stream FULL OUTER join of two aggregations (unsupported in update mode) | One aggregation over a union of both streams with conditional aggregates |
| Weather hard-coded (`rain=0`, `temp=20`) - no weather join at all | Latest Open-Meteo reading joined in `foreachBatch` |
| ±0.3x smoothing never applied (`previous_multiplier` never passed) | Previous-minute multiplier read from the store |
| CityBikes supply events had no `driver_id`, so were never counted | Removed (no CityBikes network for Hyderabad); supply = distinct AVAILABLE drivers |
| Kafka/Delta jars were Scala 2.13 on a 2.12 PySpark | `spark-sql-kafka-0-10_2.12:3.5.3` |
| Delta sink silently fell back to plain Parquet on any error | Failures surface; idempotent per-batch Parquet or Delta `txnAppId/txnVersion` |
| No Kafka `surge_updates` sink although documented | Added |
| "Latest window per zone" would be biased low | Earliest window ending at/after the newest minute boundary |

## C. Storage / pipeline bugs
- HBase: `ttl=` is not a happybase option (`time_to_live`) - table creation failed and was swallowed; data went to an in-memory dict inside the Spark process. Now fails loudly.
- Weather producer read `hourly[0]` (midnight) as "current" rain. Now uses Open-Meteo `current=`.
- Spark Core peak hours bucketed every cancelled trip into hour 0 (no pickup time). Now uses request time.
- Replay producer emitted 1000 events in ~1 s and stopped; GPS simulator pinged all drivers every 15 s regardless of supply/demand.
- Hive DDL typed `driver_id INT` but IDs are UUIDs; nothing populated the star schema. `build_star_schema.py` added.
- DLT filtered NYC coordinates on a table without lat/lon and used columns that don't exist.
- Airflow DAGs imported Spark code the Airflow image doesn't have; fixed with a custom image and BashOperator.
- Compose: `bitnami/spark:3.5.1` (Bitnami removed free versioned images), unpinned `apache/kafka:latest`, Hive containers with no config. Rewritten.
- JDBC import default port 3307 vs MySQL on 3306.

## D. What Hyderabad changed
No public Hyderabad ride-hailing dataset exists. Trips and live demand are **synthetic** (zone x hour x weekend x rain structure, in `data/hyderabad.py`); weather is real (Open-Meteo, 17.385N 78.4867E). 40 locality-centroid zones replace the 263 TLC zones; fares in INR; CityBikes supply proxy dropped.
