import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BankTransactionStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bank Transaction HDFS Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println(" Bank Transaction HDFS Streaming ")
    println("======================================")

    // Read transactions from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "bank-transactions")
      .option("startingOffsets", "latest")
      .load()

    // Extract raw JSON from Kafka
    val transactions = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json")

    // ==========================================
    // TASK 8: WRITE RAW EVENTS TO HDFS
    // ==========================================

    val query = transactions
      .writeStream
      .format("text")
      .outputMode("append")
      .option("path", "hdfs://localhost:9000/banking/raw")
      .option(
        "checkpointLocation",
        "hdfs://localhost:9000/banking/checkpoint"
      )
      .trigger(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")
      )
      .start()

    query.awaitTermination()
  }
}
