import org.apache.spark.sql.SparkSession

object FirstSparkApp {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession
      .builder()
      .appName("First Spark Application")
      .master("local[4]")
      .getOrCreate()

    // Get SparkContext
    val sc = spark.sparkContext

    println("Spark Application Started")
    println("Application Name: " + sc.appName)
    println("Master: " + sc.master)

    // Read text file as an RDD
    val lines = sc.textFile("data/input.txt")

    // Display file contents
    println("\nFile Contents:")
    lines.collect().foreach(println)

    // Stop Spark
    spark.stop()
  }
}
