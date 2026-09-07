object StudentGradeProcessor extends App {

  val students = List(
    ("Richard", 90),
    ("John", 75),
    ("Alice", 95),
    ("Bob", 60),
    ("David", 45)
  )

  println("All Students:")
  students.foreach(println)

  val passedStudents = students.filter {
    case (_, mark) => mark >= 50
  }

  println("\nPassed Students:")
  passedStudents.foreach(println)

  val grades = students.map {
    case (name, mark) =>
      val grade =
        if (mark >= 90) "A"
        else if (mark >= 80) "B"
        else if (mark >= 70) "C"
        else if (mark >= 50) "D"
        else "F"

      (name, mark, grade)
  }

  println("\nStudent Grades:")
  grades.foreach(println)

  val averageMark =
    students.map(_._2).sum.toDouble / students.size

  println("\nAverage Mark: " + averageMark)
}
