import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCSVAnalysis {

  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("Spark CSV Analysis")
      .master("local[*]")
      .getOrCreate()

    // Reduce Spark log messages
    spark.sparkContext.setLogLevel("ERROR")

    println("\n===== SPARK CSV ANALYSIS =====")

    // ------------------------------------------------
    // 1. READ CSV FILE
    // ------------------------------------------------

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n===== ORIGINAL DATA =====")
    df.show()

    // ------------------------------------------------
    // 2. PRINT SCHEMA
    // ------------------------------------------------

    println("\n===== SCHEMA =====")
    df.printSchema()

    // ------------------------------------------------
    // 3. SELECT COLUMNS
    // ------------------------------------------------

    println("\n===== NAME, DEPARTMENT AND SALARY =====")

    df.select(
      "Name",
      "Department",
      "Salary"
    ).show()

    // ------------------------------------------------
    // 4. FILTER
    // ------------------------------------------------

    println("\n===== EMPLOYEES WITH SALARY > 55000 =====")

    df.filter(
      col("Salary") > 55000
    ).show()

    // ------------------------------------------------
    // 5. FILTER BY DEPARTMENT
    // ------------------------------------------------

    println("\n===== IT DEPARTMENT =====")

    df.filter(
      col("Department") === "IT"
    ).show()

    // ------------------------------------------------
    // 6. CREATE NEW COLUMN
    // ------------------------------------------------

    println("\n===== SALARY WITH 10% BONUS =====")

    val bonusDF = df.withColumn(
      "Bonus",
      col("Salary") * 0.10
    )

    bonusDF.show()

    // ------------------------------------------------
    // 7. GROUP BY DEPARTMENT
    // ------------------------------------------------

    println("\n===== TOTAL SALARY BY DEPARTMENT =====")

    val departmentSalary = df
      .groupBy("Department")
      .agg(
        sum("Salary").alias("Total Salary")
      )

    departmentSalary.show()

    // ------------------------------------------------
    // 8. AVERAGE SALARY
    // ------------------------------------------------

    println("\n===== AVERAGE SALARY BY DEPARTMENT =====")

    val averageSalary = df
      .groupBy("Department")
      .agg(
        avg("Salary").alias("Average Salary")
      )

    averageSalary.show()

    // ------------------------------------------------
    // 9. EMPLOYEE COUNT
    // ------------------------------------------------

    println("\n===== EMPLOYEE COUNT BY DEPARTMENT =====")

    val employeeCount = df
      .groupBy("Department")
      .count()

    employeeCount.show()

    // ------------------------------------------------
    // 10. ORDER BY SALARY
    // ------------------------------------------------

    println("\n===== EMPLOYEES ORDERED BY SALARY =====")

    df.orderBy(
      col("Salary").desc
    ).show()

    // ------------------------------------------------
    // 11. WRITE RESULT
    // ------------------------------------------------

    println("\n===== WRITING RESULT =====")

    departmentSalary
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/department_salary")

    println("Result written to: output/department_salary")

    // ------------------------------------------------
    // STOP SPARK
    // ------------------------------------------------

    spark.stop()

    println("\n===== PROGRAM COMPLETED =====")
  }
}
