object ForComprehension extends App {

  val students = List(
    ("Richard", 90),
    ("John", 75),
    ("Alice", 95),
    ("Bob", 60)
  )

  // Get students who scored 80 or more
  val topStudents = for {
    (name, mark) <- students
    if mark >= 80
  } yield name

  println("Top Students:")
  println(topStudents)
}
