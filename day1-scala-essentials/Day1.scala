object Day1 extends App {

  val name = "Richard"

  var age = 22
  age = 23

  lazy val message = {
    println("Initializing message...")
    "Welcome to Scala"
  }

  println(name)
  println(age)
  println(message)
}
