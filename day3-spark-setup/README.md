# Day 3 - Spark Setup and First Application

## Topics Covered

- Apache Spark Setup
- SBT Project Configuration
- SparkSession
- SparkContext
- RDD
- Transformations
- Actions
- Partitions
- Parallel Processing
- local[2] and local[4]

## Programs

### 1. FirstSparkApp.scala

This program:

- Creates a SparkSession
- Accesses SparkContext
- Reads a text file using `sc.textFile()`
- Displays the file contents
- Runs Spark in local mode

### 2. ParallelProcessing.scala

This program:

- Creates an RDD using `parallelize`
- Uses 4 partitions
- Processes numbers in parallel
- Uses `map()` to calculate squares

## Spark Local Modes

- `local[2]` → Uses 2 threads
- `local[4]` → Uses 4 threads
- `local[*]` → Uses all available cores
