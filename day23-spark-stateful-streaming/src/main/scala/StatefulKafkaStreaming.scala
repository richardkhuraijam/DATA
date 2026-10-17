import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StatefulKafkaStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Stateful Kafka Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("          STATEFUL KAFKA STREAMING")
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

    val departmentState = transactions
      .groupBy("department")
      .agg(
        count("*").alias("transactionCount"),
        round(sum("amount"), 2).alias("totalAmount"),
        round(avg("amount"), 2).alias("averageAmount")
      )

    val query = departmentState.writeStream
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
    println("========== STATEFUL STREAMING STARTED ==========")
    println("Kafka Topic: spark-streaming-events")
    println("Maintaining department-level running totals")
    println("Processing every 10 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
