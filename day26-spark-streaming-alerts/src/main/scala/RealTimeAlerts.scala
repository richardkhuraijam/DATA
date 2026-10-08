import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RealTimeAlerts {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Real Time Transaction Alerts")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("          REAL-TIME TRANSACTION ALERTS")
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

    val alerts = transactions
      .filter(col("amount") >= 10000)
      .withColumn(
        "alertLevel",
        lit("HIGH_RISK")
      )
      .withColumn(
        "alertMessage",
        concat(
          lit("🚨 HIGH-RISK ALERT | Transaction: "),
          col("transactionId"),
          lit(" | Employee: "),
          col("employeeId"),
          lit(" | Department: "),
          col("department"),
          lit(" | Amount: ₹"),
          col("amount")
        )
      )

    val query = alerts
      .select(
        "alertLevel",
        "alertMessage"
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 20)
      .trigger(
        org.apache.spark.sql.streaming.Trigger
          .ProcessingTime("10 seconds")
      )
      .start()

    println()
    println("========== ALERT SYSTEM STARTED ==========")
    println("Kafka Topic: spark-streaming-events")
    println("Alert Threshold: ₹10,000")
    println("Processing every 10 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
