import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransactionAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day30TransactionAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val inputPath = args.headOption.getOrElse("data/transactions.csv")

    val raw = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(StructType(Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("transaction_timestamp", StringType, true),
        StructField("amount", StringType, true),
        StructField("category", StringType, true),
        StructField("status", StringType, true)
      )))
      .csv(inputPath)

    val typed = raw
      .withColumn("transaction_time", to_timestamp(col("transaction_timestamp")))
      .withColumn("amount_value", col("amount").cast(DoubleType))
      .withColumn("transaction_date", to_date(col("transaction_time")))

    val validCondition =
      col("transaction_id").isNotNull && length(trim(col("transaction_id"))) > 0 &&
      col("customer_id").isNotNull && length(trim(col("customer_id"))) > 0 &&
      col("category").isNotNull && length(trim(col("category"))) > 0 &&
      col("transaction_time").isNotNull &&
      col("amount_value").isNotNull &&
      col("amount_value") >= 0

    val clean = typed.filter(validCondition)
      .select(
        col("transaction_id"),
        col("customer_id"),
        col("transaction_time"),
        col("transaction_date"),
        col("amount_value").as("amount"),
        col("category"),
        col("status")
      )

    val dailySummary = clean
      .groupBy(col("transaction_date"))
      .agg(
        count("*").as("transaction_count"),
        round(sum("amount"), 2).as("total_amount")
      )
      .orderBy("transaction_date")

    val categorySummary = clean
      .groupBy(col("category"))
      .agg(
        count("*").as("transaction_count"),
        round(sum("amount"), 2).as("total_amount")
      )
      .orderBy(desc("total_amount"))

    val base = "output"
    clean.write.mode("overwrite").parquet(s"$base/clean_transactions")
    dailySummary.write.mode("overwrite").parquet(s"$base/daily_summary")
    categorySummary.write.mode("overwrite").parquet(s"$base/category_summary")

    println(s"Raw rows: ${raw.count()}")
    println(s"Valid rows: ${clean.count()}")
    println(s"Rejected rows: ${raw.count() - clean.count()}")
    println("Daily summary:")
    dailySummary.show(truncate = false)
    println("Category summary:")
    categorySummary.show(truncate = false)

    spark.stop()
  }
}
