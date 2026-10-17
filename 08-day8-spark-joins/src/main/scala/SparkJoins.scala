import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJoins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 8 - Spark Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // STEP 1: Read Employees
    // ------------------------------------------------------------

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n========== EMPLOYEES ==========")
    employees.show()

    // ------------------------------------------------------------
    // STEP 2: Read Departments
    // ------------------------------------------------------------

    val departments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/departments.csv")

    println("\n========== DEPARTMENTS ==========")
    departments.show()

    // ------------------------------------------------------------
    // STEP 3: INNER JOIN
    // ------------------------------------------------------------

    println("\n========== INNER JOIN ==========")

    val innerJoin = employees.join(
      departments,
      employees("DepartmentID") === departments("DepartmentID"),
      "inner"
    )

    innerJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        departments("DepartmentName"),
        departments("Location"),
        employees("Salary")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 4: LEFT JOIN
    // ------------------------------------------------------------

    println("\n========== LEFT JOIN ==========")

    val leftJoin = employees.join(
      departments,
      employees("DepartmentID") === departments("DepartmentID"),
      "left"
    )

    leftJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        employees("DepartmentID"),
        departments("DepartmentName")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 5: RIGHT JOIN
    // ------------------------------------------------------------

    println("\n========== RIGHT JOIN ==========")

    val rightJoin = employees.join(
      departments,
      employees("DepartmentID") === departments("DepartmentID"),
      "right"
    )

    rightJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        departments("DepartmentID"),
        departments("DepartmentName")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 6: FULL OUTER JOIN
    // ------------------------------------------------------------

    println("\n========== FULL OUTER JOIN ==========")

    val fullJoin = employees.join(
      departments,
      employees("DepartmentID") === departments("DepartmentID"),
      "full"
    )

    fullJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        coalesce(
          employees("DepartmentID"),
          departments("DepartmentID")
        ).alias("DepartmentID"),
        departments("DepartmentName")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 7: JOIN + FILTER
    // ------------------------------------------------------------

    println("\n========== IT EMPLOYEES ==========")

    innerJoin
      .filter(col("DepartmentName") === "IT")
      .select(
        employees("Name"),
        employees("Salary"),
        departments("Location")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 8: JOIN + GROUP BY
    // ------------------------------------------------------------

    println("\n========== AVERAGE SALARY BY DEPARTMENT ==========")

    innerJoin
      .groupBy(departments("DepartmentName"))
      .agg(
        count(employees("EmployeeID")).alias("EmployeeCount"),
        avg(employees("Salary")).alias("AverageSalary"),
        sum(employees("Salary")).alias("TotalSalary")
      )
      .orderBy(desc("AverageSalary"))
      .show()

    // ------------------------------------------------------------
    // STEP 9: DEPARTMENTS WITHOUT EMPLOYEES
    // ------------------------------------------------------------

    println("\n========== DEPARTMENTS WITHOUT EMPLOYEES ==========")

    departments
      .join(
        employees,
        departments("DepartmentID") === employees("DepartmentID"),
        "left"
      )
      .filter(employees("EmployeeID").isNull)
      .select(
        departments("DepartmentID"),
        departments("DepartmentName"),
        departments("Location")
      )
      .show()

    // ------------------------------------------------------------
    // STEP 10: HIGH SALARY EMPLOYEES WITH DEPARTMENT
    // ------------------------------------------------------------

    println("\n========== HIGH SALARY EMPLOYEES ==========")

    innerJoin
      .filter(employees("Salary") > 55000)
      .select(
        employees("Name"),
        employees("Salary"),
        departments("DepartmentName")
      )
      .orderBy(desc("Salary"))
      .show()

    spark.stop()
  }
}
