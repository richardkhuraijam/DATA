import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Kafka Structured Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("       SPARK STRUCTURED STREAMING + KAFKA")
    println("======================================================")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-streaming-events")
      .option("startingOffsets", "latest")
      .load()

    println()
    println("========== KAFKA STREAM CONNECTED ==========")

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
        from_json(col("json"), transactionSchema)
          .alias("data")
      )
      .select("data.*")

    val processedTransactions = transactions
      .withColumn(
        "transactionType",
        when(col("amount") >= 7000, "High")
          .when(col("amount") >= 4000, "Medium")
          .otherwise("Low")
      )

    val departmentSummary = processedTransactions
      .groupBy("department")
      .agg(
        count("*").alias("transactionCount"),
        round(sum("amount"), 2).alias("totalAmount"),
        round(avg("amount"), 2).alias("averageAmount")
      )

    val query = departmentSummary.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 20)
      .trigger(
        org.apache.spark.sql.streaming.Trigger
          .ProcessingTime("5 seconds")
      )
      .start()

    println()
    println("========== STREAMING STARTED ==========")
    println("Listening to Kafka topic: spark-streaming-events")
    println("Processing every 5 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
