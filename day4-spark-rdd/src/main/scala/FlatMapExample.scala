import org.apache.spark.sql.SparkSession

object FlatMapExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("FlatMap Example")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val sentences = sc.parallelize(
      List(
        "Spark is fast",
        "Scala is powerful",
        "Spark processes big data"
      )
    )

    // Split sentences into words
    val words = sentences.flatMap(sentence => sentence.split(" "))

    println("Words:")

    words.collect().foreach(println)

    spark.stop()
  }
}
