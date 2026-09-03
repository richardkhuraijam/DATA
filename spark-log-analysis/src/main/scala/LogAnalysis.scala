import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LogAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Log Analysis")
      .master("local[*]")
      .getOrCreate()

    println("\n========== SPARK STARTED ==========\n")

    // Read log file
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("data/application.log")

    // Rename columns
    val logs = df.toDF(
      "timestamp",
      "level",
      "ip",
      "url",
      "status",
      "response_time"
    )

    // Convert timestamp
    val cleanLogs = logs.withColumn(
      "timestamp",
      to_timestamp(
        col("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
      )
    )

    // Display data
    println("========== LOG DATA ==========")
    cleanLogs.show(false)

    // Schema
    println("========== SCHEMA ==========")
    cleanLogs.printSchema()

    // Total requests
    val totalRequests = cleanLogs.count()

    println(s"Total Requests = $totalRequests")

    // Total errors
    val totalErrors = cleanLogs
      .filter(col("level") === "ERROR")
      .count()

    println(s"Total Errors = $totalErrors")

    // Error logs
    println("\n========== ERROR LOGS ==========")

    val errors = cleanLogs
      .filter(col("level") === "ERROR")

    errors.show(false)

    // Most accessed URLs
    println("\n========== MOST ACCESSED URLs ==========")

    val urlCount = cleanLogs
      .groupBy("url")
      .count()
      .orderBy(desc("count"))

    urlCount.show()
    // Most active IP addresses
println("\n========== MOST ACTIVE IPs ==========")

val ipCount = cleanLogs
  .groupBy("ip")
  .count()
  .orderBy(desc("count"))

ipCount.show()
// Average response time
println("\n========== AVERAGE RESPONSE TIME ==========")

val avgResponseTime = cleanLogs
  .select(
    avg("response_time")
      .alias("average_response_time")
  )

avgResponseTime.show()  
// Slow requests
println("\n========== SLOW REQUESTS ==========")

val slowRequests = cleanLogs
  .filter(col("response_time") > 400)

slowRequests.show(false)
// Average response time by URL
println("\n========== URL PERFORMANCE ==========")

val urlPerformance = cleanLogs
  .groupBy("url")
  .agg(
    avg("response_time")
      .alias("avg_response_time")
  )
  .orderBy(desc("avg_response_time"))

urlPerformance.show()
// Errors by URL
println("\n========== ERRORS BY URL ==========")

val errorsByUrl = cleanLogs
  .filter(col("level") === "ERROR")
  .groupBy("url")
  .count()
  .orderBy(desc("count"))

errorsByUrl.show()

// ---------------------------------------------
// 17. HTTP Status Analysis
// --------------------------------------------

println("\n========== HTTP STATUS ANALYSIS ==========")

val statusAnalysis = cleanLogs
  .groupBy("status")
  .count()
  .orderBy("status")

statusAnalysis.show()


// ---------------------------------------------
// 18. Error Percentage
// --------------------------------------------

println("\n========== ERROR PERCENTAGE ==========")

val summary = cleanLogs
  .agg(
    count("*")
      .alias("total_requests"),

    count(
      when(
        col("level") === "ERROR",
        true
      )
    ).alias("total_errors")
  )
  .withColumn(
    "error_percentage",
    col("total_errors") /
      col("total_requests") * 100
  )

summary.show()


// ---------------------------------------------
// 19. Stop Spark
// --------------------------------------------

spark.stop()

println("\n========== SPARK FINISHED ==========\n")  // Stop Spark
    spark.stop()
  }
}
