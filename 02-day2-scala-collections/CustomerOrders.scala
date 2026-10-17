object CustomerOrders extends App {

  val customers = List(
    ("C001", "Richard"),
    ("C002", "John"),
    ("C003", "Alice")
  )

  val orders = List(
    ("C001", "Laptop", 50000),
    ("C002", "Phone", 30000),
    ("C001", "Headphones", 5000),
    ("C003", "Tablet", 20000)
  )

  // Combine customers with their orders
  val customerOrders = for {
    (customerId, customerName) <- customers
    (orderCustomerId, product, price) <- orders
    if customerId == orderCustomerId
  } yield (customerName, product, price)

  println("Customer Orders:")

  customerOrders.foreach(println)
}
