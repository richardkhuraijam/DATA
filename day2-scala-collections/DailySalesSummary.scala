object DailySalesSummary extends App {

  val sales = List(
    ("Laptop", 50000),
    ("Phone", 30000),
    ("Laptop", 45000),
    ("Tablet", 20000),
    ("Phone", 25000),
    ("Headphones", 5000)
  )

  println("All Sales:")
  sales.foreach(println)

  // Calculate total revenue
  val totalRevenue = sales.map(_._2).sum

  println("\nTotal Revenue: " + totalRevenue)

  // Filter high-value sales
  val highValueSales = sales.filter {
    case (_, amount) => amount >= 30000
  }

  println("\nHigh Value Sales:")
  highValueSales.foreach(println)

  // Calculate total revenue for each product
  val productRevenue = sales
    .groupBy(_._1)
    .map {
      case (product, transactions) =>
        product -> transactions.map(_._2).sum
    }

  println("\nRevenue by Product:")
  productRevenue.foreach(println)

  // Find the highest sale
  val highestSale = sales.maxBy(_._2)

  println("\nHighest Sale: " + highestSale)
}
