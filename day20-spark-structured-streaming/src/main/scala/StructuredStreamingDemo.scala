import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Structured Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("======================================================")
    println("          SPARK STRUCTURED STREAMING")
    println("======================================================")

    // --------------------------------------------------
    // 1. DEFINE SCHEMA
    // --------------------------------------------------

    val transactionSchema = StructType(Seq(
      StructField("TransactionID", StringType, true),
      StructField("EmployeeID", StringType, true),
      StructField("Department", StringType, true),
      StructField("Amount", IntegerType, true)
    ))

    // --------------------------------------------------
    // 2. READ STREAM
    // --------------------------------------------------

    val inputPath = "data/input"

    val transactions = spark.readStream
      .schema(transactionSchema)
      .option("header", "true")
      .csv(inputPath)

    println()
    println("========== STREAMING DATA SCHEMA ==========")

    transactions.printSchema()

    // --------------------------------------------------
    // 3. TRANSFORMATION
    // --------------------------------------------------

    val processedTransactions = transactions
      .withColumn(
        "TransactionType",
        when(col("Amount") >= 7000, "High")
          .when(col("Amount") >= 4000, "Medium")
          .otherwise("Low")
      )

    // --------------------------------------------------
    // 4. AGGREGATION
    // --------------------------------------------------

    val departmentSummary = processedTransactions
      .groupBy("Department")
      .agg(
        count("*").alias("TransactionCount"),
        round(sum("Amount"), 2).alias("TotalAmount"),
        round(avg("Amount"), 2).alias("AverageAmount")
      )

    // --------------------------------------------------
    // 5. WRITE STREAM
    // --------------------------------------------------

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
    println("========== STREAM STARTED ==========")
    println("Processing new CSV files every 5 seconds...")
    println("Press Ctrl+C to stop.")
    println()

    query.awaitTermination()
  }
}
