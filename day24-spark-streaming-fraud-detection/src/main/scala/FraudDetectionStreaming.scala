import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FraudDetectionStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Real Time Fraud Detection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("          REAL-TIME FRAUD DETECTION")
    println("======================================================")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-streaming-events")
      .option("startingOffsets", "latest")
      .load()

    val jsonData = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")

    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, true),
      StructField("employeeId", StringType, true),
      StructField("department", StringType, true),
      StructField("amount", DoubleType, true)
    ))

    val transactions = jsonData
      .select(
        from_json(col("json"), transactionSchema).alias("data")
      )
      .select("data.*")

    val fraudDetection = transactions
      .withColumn(
        "riskLevel",
        when(col("amount") >= 10000, "HIGH_RISK")
          .when(col("amount") >= 5000, "SUSPICIOUS")
          .otherwise("NORMAL")
      )

    val fraudSummary = fraudDetection
      .groupBy("riskLevel")
      .agg(
        count("*").alias("transactionCount"),
        round(sum("amount"), 2).alias("totalAmount"),
        round(avg("amount"), 2).alias("averageAmount")
      )

    val query = fraudSummary.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 20)
      .trigger(
        org.apache.spark.sql.streaming.Trigger
          .ProcessingTime("10 seconds")
      )
      .start()

    println()
    println("========== FRAUD DETECTION STARTED ==========")
    println("Kafka Topic: spark-streaming-events")
    println("Risk Rules:")
    println("  < 5000       = NORMAL")
    println("  5000-9999    = SUSPICIOUS")
    println("  >= 10000     = HIGH_RISK")
    println("Processing every 10 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
