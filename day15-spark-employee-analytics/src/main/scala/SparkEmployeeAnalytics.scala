import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkEmployeeAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Employee Analytics")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println()
    println("========== EMPLOYEE ANALYTICS ==========")

    // Employee data
    val employees = Seq(
      (1, "Alice", 101, 45000),
      (2, "Bob", 102, 55000),
      (3, "Charlie", 101, 60000),
      (4, "David", 103, 50000),
      (5, "Eva", 104, 65000),
      (6, "Frank", 102, 58000),
      (7, "Grace", 101, 62000),
      (8, "Helen", 103, 52000)
    ).toDF(
      "EmployeeID",
      "Name",
      "DepartmentID",
      "Salary"
    )

    // Department data
    val departments = Seq(
      (101, "IT", "Imphal"),
      (102, "Finance", "Guwahati"),
      (103, "HR", "Delhi"),
      (104, "Marketing", "Bangalore")
    ).toDF(
      "DepartmentID",
      "DepartmentName",
      "Location"
    )

    println()
    println("========== EMPLOYEES ==========")
    employees.show()

    println()
    println("========== DEPARTMENTS ==========")
    departments.show()

    // Join employees with departments
    val employeeDetails = employees
      .join(
        departments,
        employees("DepartmentID") === departments("DepartmentID"),
        "inner"
      )
      .select(
        employees("EmployeeID"),
        employees("Name"),
        departments("DepartmentName"),
        departments("Location"),
        employees("Salary")
      )

    println()
    println("========== EMPLOYEE DETAILS ==========")
    employeeDetails.show()

    // Department salary statistics
    println()
    println("========== DEPARTMENT SALARY ANALYSIS ==========")

    val departmentAnalysis = employeeDetails
      .groupBy("DepartmentName")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary"),
        round(avg("Salary"), 2).alias("AverageSalary"),
        max("Salary").alias("HighestSalary")
      )
      .orderBy(desc("TotalSalary"))

    departmentAnalysis.show()

    // Employees earning more than 55000
    println()
    println("========== HIGH SALARY EMPLOYEES ==========")

    employeeDetails
      .filter(col("Salary") > 55000)
      .orderBy(desc("Salary"))
      .show()

    // Average salary across company
    println()
    println("========== COMPANY AVERAGE SALARY ==========")

    employeeDetails
      .agg(
        round(avg("Salary"), 2).alias("CompanyAverageSalary")
      )
      .show()

    // Highest-paid employee
    println()
    println("========== HIGHEST PAID EMPLOYEE ==========")

    employeeDetails
      .orderBy(desc("Salary"))
      .limit(1)
      .show()

    // Employees grouped by location
    println()
    println("========== EMPLOYEE COUNT BY LOCATION ==========")

    employeeDetails
      .groupBy("Location")
      .agg(
        count("*").alias("EmployeeCount")
      )
      .orderBy(desc("EmployeeCount"))
      .show()

    println()
    println("========== PROGRAM COMPLETED ==========")

    spark.stop()
  }
}
