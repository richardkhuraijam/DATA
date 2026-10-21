# Day 25 — Spark Execution, DAG, Jobs, Stages, Tasks & Shuffle

A hands-on Scala + Apache Spark project for the 30-day learning journey.

## Learning objectives
- Distinguish transformations from actions.
- Understand how Spark builds a Directed Acyclic Graph (DAG).
- Observe jobs, stages, and tasks in Spark output and the Spark UI.
- Inspect a physical execution plan with `explain(true)`.
- See how filtering, grouping, and shuffling affect a batch analytics pipeline.
- Cache a reused DataFrame and compare the plan.

## Stack
- Java 17
- Scala 2.12.18
- Apache Spark SQL 3.5.3
- sbt 1.10.0

## Project structure
```text
day25-spark-execution-dag/
├── README.md
├── build.sbt
├── .gitignore
├── project/
│   └── build.properties
├── data/
│   └── transactions.csv
└── src/
    └── main/
        └── scala/
            └── SparkExecutionDag.scala
```

## Run
From this project directory:
```bash
sbt run
```

The application reads `data/transactions.csv`, filters successful transactions, adds a derived amount band, prints a category summary, and prints the query execution plan. It also demonstrates caching a DataFrame reused by two actions.

## What to look for
1. `filter`, `select`, and `withColumn` are transformations; they are lazy.
2. `show`, `count`, and `collect` are actions that trigger execution.
3. Spark can combine transformations before executing a job.
4. Aggregations such as `groupBy` commonly require a shuffle.
5. The physical plan may show operators such as `Exchange`, `HashAggregate`, and `FileScan`.
6. Spark UI is usually available at `http://localhost:4040` while the app is running. In local mode, jobs and stages are visible there.

## Expected result
The category summary should contain successful transactions only. Exact physical-plan details can vary by Spark version and optimizer settings.

## Notes
This is a local learning/demo dataset, not a production pipeline. If your local Java setup requires extra module access, the `build.sbt` already includes common Java 17 options for the run task.
