import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RealtimeFraudDetection {
  def main(args: Array[String]): Unit = {
    val bootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "transactions")

    val spark = SparkSession.builder()
      .appName("Day28RealtimeFraudDetection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, nullable = true),
      StructField("account_id", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("merchant", StringType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("event_time", StringType, nullable = true),
      StructField("is_international", BooleanType, nullable = true)
    ))

    val transactions = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), transactionSchema).as("txn"))
      .select("txn.*")
      .withColumn("event_timestamp", to_timestamp(col("event_time")))

    val suspiciousTransactions = transactions
      .filter(
        col("amount").isNotNull &&
        (col("amount") >= 100000.0 || coalesce(col("is_international"), lit(false)))
      )
      .select(
        col("transaction_id"),
        col("account_id"),
        col("amount"),
        col("merchant"),
        col("country"),
        col("event_timestamp"),
        col("is_international"),
        when(col("amount") >= 100000.0, lit("HIGH_VALUE"))
          .when(coalesce(col("is_international"), lit(false)), lit("INTERNATIONAL"))
          .otherwise(lit("REVIEW"))
          .as("alert_reason")
      )

    val query = suspiciousTransactions.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 50)
      .option("checkpointLocation", "/tmp/day28-realtime-fraud-checkpoint")
      .start()

    println(s"Day 28 fraud detection started. Kafka=$bootstrapServers, topic=$topic")
    query.awaitTermination()
  }
}
