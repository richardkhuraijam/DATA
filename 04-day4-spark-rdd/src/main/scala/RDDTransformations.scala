import org.apache.spark.sql.SparkSession

object RDDTransformations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RDD Transformations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create an RDD
    val numbers = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // map transformation
    val squares = numbers.map(number => number * number)

    // filter transformation
    val evenNumbers = numbers.filter(number => number % 2 == 0)

    println("Original Numbers:")
    numbers.collect().foreach(println)

    println("\nSquares:")
    squares.collect().foreach(println)

    println("\nEven Numbers:")
    evenNumbers.collect().foreach(println)

    spark.stop()
  }
}
