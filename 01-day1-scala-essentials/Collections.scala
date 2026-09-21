object Collections extends App {

  // List - ordered collection
  val students = List("Richard", "John", "Alice")
  println("List: " + students)

  // Vector - efficient indexed collection
  val numbers = Vector(10, 20, 30, 40)
  println("Vector: " + numbers)
  println("First number: " + numbers(0))

  // Set - stores unique values
  val subjects = Set("Scala", "Spark", "Scala", "SQL")
  println("Set: " + subjects)

  // Map - key-value pairs
  val marks = Map(
    "Richard" -> 90,
    "John" -> 85,
    "Alice" -> 95
  )

  println("Marks: " + marks)
  println("Richard's mark: " + marks("Richard"))
}
