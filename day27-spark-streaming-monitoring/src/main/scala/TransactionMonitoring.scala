import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransactionMonitoring {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Real Time Transaction Monitoring")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("       REAL-TIME TRANSACTION MONITORING")
    println("======================================================")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-streaming-events")
      .option("startingOffsets", "latest")
      .load()

    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, true),
      StructField("employeeId", StringType, true),
      StructField("department", StringType, true),
      StructField("amount", DoubleType, true)
    ))

    val transactions = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")
      .select(
        from_json(col("json"), transactionSchema).alias("data")
      )
      .select("data.*")

    val monitoredTransactions = transactions
      .withColumn(
        "riskLevel",
        when(col("amount") >= 10000, "HIGH_RISK")
          .when(col("amount") >= 5000, "SUSPICIOUS")
          .otherwise("NORMAL")
      )

    val monitoringSummary = monitoredTransactions
      .groupBy("department", "riskLevel")
      .agg(
        count("*").alias("transactionCount"),
        round(sum("amount"), 2).alias("totalAmount"),
        round(avg("amount"), 2).alias("averageAmount")
      )
      .orderBy("department", "riskLevel")

    val query = monitoringSummary
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 50)
      .trigger(
        org.apache.spark.sql.streaming.Trigger
          .ProcessingTime("10 seconds")
      )
      .start()

    println()
    println("========== MONITORING SYSTEM STARTED ==========")
    println("Kafka Topic: spark-streaming-events")
    println("Monitoring transactions in real time...")
    println("Processing every 10 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
