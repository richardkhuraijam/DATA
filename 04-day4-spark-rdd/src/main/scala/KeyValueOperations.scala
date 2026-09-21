import org.apache.spark.sql.SparkSession

object KeyValueOperations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Key Value Operations")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    val sc = spark.sparkContext

    // Key-Value RDD
    val sales = sc.parallelize(
      List(
        ("Laptop", 1000),
        ("Phone", 500),
        ("Laptop", 1500),
        ("Phone", 700),
        ("Tablet", 300)
      )
    )

    // reduceByKey - adds values for each key
    val totalSales = sales.reduceByKey(_ + _)

    println("Total Sales using reduceByKey:")

    totalSales.collect().foreach(println)

    // groupByKey - groups values for each key
    val groupedSales = sales.groupByKey()

    println("\nGrouped Sales using groupByKey:")

    groupedSales.collect().foreach {
      case (product, amounts) =>
        println(product + " -> " + amounts.mkString(", "))
    }

    spark.stop()
  }
}
