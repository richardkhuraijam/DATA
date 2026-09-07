import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hospital Billing Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n======================================")
    println("STEP 46 - STRUCTURED STREAMING")
    println("======================================")

    val paymentSchema = StructType(Seq(
      StructField("payment_id", StringType, true),
      StructField("visit_id", StringType, true),
      StructField("payment_date", StringType, true),
      StructField("payment_amount", DoubleType, true),
      StructField("payment_method", StringType, true)
    ))

    val streamingPayments = spark.readStream
      .option("header", "true")
      .schema(paymentSchema)
      .csv("data/streaming/input")

    val processedPayments = streamingPayments
      .withColumn(
        "payment_category",
        when(col("payment_amount") >= 2000, "HIGH")
          .otherwise("NORMAL")
      )

    val query = processedPayments.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("checkpointLocation", "data/streaming/checkpoint")
      .start()

    println("Streaming query started...")
    println("Watching: data/streaming/input")

    query.awaitTermination()
  }
}
