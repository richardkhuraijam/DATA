import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WindowedKafkaStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Kafka Windowed Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("        KAFKA WINDOWED STREAMING ANALYTICS")
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
      .withColumn("eventTime", current_timestamp())

    val windowedSummary = transactions
      .withWatermark("eventTime", "1 minute")
      .groupBy(
        window(col("eventTime"), "1 minute"),
        col("department")
      )
      .agg(
        count("*").alias("transactionCount"),
        round(sum("amount"), 2).alias("totalAmount"),
        round(avg("amount"), 2).alias("averageAmount")
      )
      .select(
        col("window.start").alias("windowStart"),
        col("window.end").alias("windowEnd"),
        col("department"),
        col("transactionCount"),
        col("totalAmount"),
        col("averageAmount")
      )

    val query = windowedSummary.writeStream
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
    println("========== WINDOWED STREAMING STARTED ==========")
    println("Kafka Topic: spark-streaming-events")
    println("Window: 1 minute")
    println("Watermark: 1 minute")
    println("Trigger: 10 seconds")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
