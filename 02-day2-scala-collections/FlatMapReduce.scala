object FlatMapReduce extends App {

  val sentences = List(
    "Scala is powerful",
    "Spark is fast",
    "Scala and Spark"
  )

  // flatMap: split sentences into individual words
  val words = sentences.flatMap(sentence => sentence.split(" "))

  println("Words:")
  println(words)

  // reduce: combine all numbers into one result
  val sales = List(100, 200, 300, 400)

  val totalSales = sales.reduce((a, b) => a + b)

  println("Total Sales: " + totalSales)
}
