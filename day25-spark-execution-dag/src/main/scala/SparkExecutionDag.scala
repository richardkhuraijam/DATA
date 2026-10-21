import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkExecutionDag {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day25-Spark-Execution-DAG")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val inputPath = "data/transactions.csv"
      val transactions = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)

      println("\\n=== 1. INPUT DATA ===")
      transactions.show(truncate = false)
      println(s"Input row count: ${transactions.count()}")

      // Transformations are lazy until an action is called.
      val successful = transactions
        .filter(col("status") === "SUCCESS")
        .withColumn(
          "amount_band",
          when(col("amount") >= 500, "HIGH")
            .when(col("amount") >= 200, "MEDIUM")
            .otherwise("LOW")
        )
        .select("transaction_id", "category", "amount", "city", "amount_band")
        .cache()

      println("\\n=== 2. TRANSFORMATION LINEAGE / LOGICAL PLAN ===")
      successful.explain(extended = true)

      println("\\n=== 3. ACTION: COUNT SUCCESSFUL TRANSACTIONS ===")
      println(s"Successful transactions: ${successful.count()}")

      println("\\n=== 4. ACTION: CATEGORY SUMMARY ===")
      val categorySummary = successful
        .groupBy("category")
        .agg(
          count("*").as("transaction_count"),
          round(sum("amount"), 2).as("total_amount")
        )
        .orderBy(desc("total_amount"))

      categorySummary.show(truncate = false)

      println("\\n=== 5. ACTION: AMOUNT BAND SUMMARY ===")
      successful.groupBy("amount_band")
        .agg(count("*").as("transaction_count"), round(sum("amount"), 2).as("total_amount"))
        .orderBy("amount_band")
        .show(truncate = false)

      println("\\n=== 6. PHYSICAL PLAN (LOOK FOR EXCHANGE / AGGREGATE OPERATORS) ===")
      categorySummary.explain(extended = true)

      println("\\n=== 7. SPARK EXECUTION CONCEPTS ===")
      println("Transformation: filter / withColumn / select / groupBy")
      println("Actions: show / count")
      println("DAG: Spark's dependency graph of transformations")
      println("Job: work triggered by an action")
      println("Stage: a group of tasks separated by shuffle boundaries")
      println("Task: work on one partition")
      println("Shuffle: redistributing data across partitions, often for aggregation")

      successful.unpersist()
    } finally {
      spark.stop()
    }
  }
}
