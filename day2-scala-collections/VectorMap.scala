object VectorMap extends App {

  // Vector: efficient indexed collection
  val customers = Vector(
    "Richard",
    "John",
    "Alice",
    "Bob"
  )

  println("All Customers: " + customers)

  // Accessing elements by index
  println("First Customer: " + customers(0))
  println("Third Customer: " + customers(2))


  // Map: key-value collection
  val products = Map(
    "Laptop" -> 50000,
    "Phone" -> 30000,
    "Tablet" -> 20000
  )

  println("\nProducts and Prices:")
  products.foreach(println)

  // Access a value using its key
  println("\nLaptop Price: " + products("Laptop"))
}
