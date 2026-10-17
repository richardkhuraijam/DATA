import org.apache.spark.sql.SparkSession

object SparkBroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Broadcast and Accumulator")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    println()
    println("========== SPARK BROADCAST VARIABLE ==========")

    // Read-only lookup data shared with all executors
    val departmentMap = Map(
      101 -> "IT",
      102 -> "Finance",
      103 -> "HR",
      104 -> "Marketing"
    )

    val broadcastDepartments = sc.broadcast(departmentMap)

    val employees = Seq(
      (1, "Alice", 101, 45000),
      (2, "Bob", 102, 55000),
      (3, "Charlie", 101, 60000),
      (4, "David", 103, 50000),
      (5, "Eva", 104, 65000),
      (6, "Frank", 102, 58000)
    )

    val employeeRDD = sc.parallelize(employees)

    val employeeWithDepartment = employeeRDD.map {
      case (id, name, departmentId, salary) =>
        val department =
          broadcastDepartments.value.getOrElse(
            departmentId,
            "Unknown"
          )

        (id, name, department, salary)
    }

    employeeWithDepartment.collect().foreach {
      case (id, name, department, salary) =>
        println(
          s"ID: $id | Name: $name | Department: $department | Salary: $salary"
        )
    }

    println()
    println("========== SPARK ACCUMULATOR ==========")

    // Accumulator for counting high-salary employees
    val highSalaryCount = sc.longAccumulator("High Salary Employees")

    employeeRDD.foreach {
      case (_, _, _, salary) =>
        if (salary >= 60000) {
          highSalaryCount.add(1)
        }
    }

    println(
      s"Employees with salary >= 60000: ${highSalaryCount.value}"
    )

    println()
    println("========== SALARY ACCUMULATOR ==========")

    // Accumulator for calculating total salary
    val totalSalary = sc.longAccumulator("Total Salary")

    employeeRDD.foreach {
      case (_, _, _, salary) =>
        totalSalary.add(salary)
    }

    println(
      s"Total Salary: ${totalSalary.value}"
    )

    println()
    println("========== COMBINED ANALYSIS ==========")

    val highSalaryEmployees =
      employeeRDD
        .filter {
          case (_, _, _, salary) =>
            salary >= 60000
        }

    highSalaryEmployees.collect().foreach {
      case (id, name, departmentId, salary) =>
        val department =
          broadcastDepartments.value.getOrElse(
            departmentId,
            "Unknown"
          )

        println(
          s"$name -> $department -> Salary: $salary"
        )
    }

    println()
    println("========== PROGRAM COMPLETED ==========")

    broadcastDepartments.destroy()

    spark.stop()
  }
}
