object SalesOperations extends App {

  val sales = List(100, 250, 75, 400, 150)

  // map: transform every element
  val increasedSales = sales.map(sale => sale * 2)

  println("Original Sales: " + sales)
  println("Doubled Sales: " + increasedSales)

  // filter: select elements based on a condition
  val highSales = sales.filter(sale => sale >= 150)

  println("High Sales: " + highSales)
}
