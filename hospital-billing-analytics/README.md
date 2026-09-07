# Hospital Billing and Revenue Analytics

## Overview

This project uses Apache Spark with Scala to process hospital billing data
and calculate revenue, insurance coverage, payments, and outstanding balances.

## Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- Structured Streaming
- SBT
- Java 17

## Batch Processing

The batch processor combines:

- Patient visits
- Procedures
- Insurance coverage
- Payments

It calculates:

- Total bill
- Insurance covered amount
- Amount paid
- Outstanding balance
- Billing status

## Spark Concepts Demonstrated

- DataFrame operations
- Spark SQL
- Multi-table joins
- GROUP BY / HAVING
- UDF
- Dataset
- RDD
- Pair RDD
- map
- filter
- reduceByKey
- groupByKey
- sortByKey
- Narrow transformations
- Wide transformations
- Shuffle
- DAG and stages
- Partitions
- repartition
- coalesce
- Broadcast variables
- Accumulator
- Cache
- Persist
- Partitioned output

## Structured Streaming

The streaming processor monitors:

data/streaming/input

New payment files are processed using Spark Structured Streaming.

Checkpointing is enabled for fault tolerance:

data/streaming/checkpoint

## Production Architecture

In a production environment, Spark can run on a cluster using YARN.

### Driver

The Driver:

- Creates the SparkSession
- Builds the execution plan
- Creates jobs and stages
- Coordinates executors

### Executors

Executors:

- Execute tasks
- Process partitions
- Store cached data
- Return results to the Driver

### YARN

YARN can manage cluster resources by allocating resources
to Spark applications.

## Fault Tolerance

Spark provides fault tolerance through:

- RDD lineage
- Task retry
- Partition recomputation
- Checkpointing for streaming workloads

## Output

Batch billing records are written as partitioned CSV files:

data/output/billing_records

The output is partitioned by:

department

## Running the Project

Compile:

sbt compile

Run batch processing:

sbt "runMain BatchProcessor"

Run streaming:

sbt "runMain StreamingProcessor"
