import org.apache.spark.sql.SparkSession

object ParallelProcessing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Parallel Processing Demo")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create an RDD with numbers
    val numbers = sc.parallelize(1 to 20, 4)

    println("Number of Partitions: " + numbers.getNumPartitions)

    // Process data in parallel
    val squares = numbers.map(number => number * number)

    println("\nSquares:")

    squares.collect().foreach(println)

    spark.stop()
  }
}
