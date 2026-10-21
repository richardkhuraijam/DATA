import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day29StreamingAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventSchema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("event_time", TimestampType, nullable = true),
      StructField("page", StringType, nullable = true)
    ))

    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clickstream-events")
      .option("startingOffsets", "latest")
      .load()
      .select(from_json(col("value").cast("string"), eventSchema).as("event"))
      .select("event.*")
      .filter(col("event_id").isNotNull && col("event_time").isNotNull && col("event_type").isNotNull)

    val metrics = events
      .withWatermark("event_time", "1 minute")
      .groupBy(window(col("event_time"), "1 minute"), col("event_type"))
      .count()
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("event_type"),
        col("count")
      )

    val query = metrics.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/tmp/day29-streaming-analytics-checkpoint")
      .start()

    query.awaitTermination()
  }
}
