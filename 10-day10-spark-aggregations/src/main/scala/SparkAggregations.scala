import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkAggregations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 10 - Spark Aggregations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // STEP 1: READ DATA
    // ------------------------------------------------------------

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n========== EMPLOYEES ==========")
    employees.show()

    // ------------------------------------------------------------
    // STEP 2: BASIC GROUP BY
    // ------------------------------------------------------------

    println("\n========== EMPLOYEE COUNT BY DEPARTMENT ==========")

    employees
      .groupBy("Department")
      .count()
      .orderBy(desc("count"))
      .show()

    // ------------------------------------------------------------
    // STEP 3: MULTIPLE AGGREGATIONS
    // ------------------------------------------------------------

    println("\n========== DEPARTMENT SALARY ANALYSIS ==========")

    employees
      .groupBy("Department")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary"),
        avg("Salary").alias("AverageSalary"),
        min("Salary").alias("MinimumSalary"),
        max("Salary").alias("MaximumSalary")
      )
      .orderBy(desc("AverageSalary"))
      .show()

    // ------------------------------------------------------------
    // STEP 4: CONDITIONAL COLUMN
    // ------------------------------------------------------------

    println("\n========== SALARY CATEGORIES ==========")

    val categorizedEmployees = employees.withColumn(
      "SalaryCategory",
      when(col("Salary") >= 65000, "High")
        .when(col("Salary") >= 55000, "Medium")
        .otherwise("Low")
    )

    categorizedEmployees
      .select(
        "Name",
        "Department",
        "Salary",
        "SalaryCategory"
      )
      .orderBy(desc("Salary"))
      .show()

    // ------------------------------------------------------------
    // STEP 5: COUNT EMPLOYEES BY SALARY CATEGORY
    // ------------------------------------------------------------

    println("\n========== EMPLOYEES BY SALARY CATEGORY ==========")

    categorizedEmployees
      .groupBy("SalaryCategory")
      .count()
      .orderBy(desc("count"))
      .show()

    // ------------------------------------------------------------
    // STEP 6: CONDITIONAL AGGREGATION
    // ------------------------------------------------------------

    println("\n========== HIGH SALARY EMPLOYEES BY DEPARTMENT ==========")

    employees
      .groupBy("Department")
      .agg(
        sum(
          when(col("Salary") >= 65000, 1)
            .otherwise(0)
        ).alias("HighSalaryEmployees"),
        count("*").alias("TotalEmployees")
      )
      .orderBy(desc("HighSalaryEmployees"))
      .show()

    // ------------------------------------------------------------
    // STEP 7: AVERAGE EXPERIENCE
    // ------------------------------------------------------------

    println("\n========== EXPERIENCE ANALYSIS ==========")

    employees
      .groupBy("Department")
      .agg(
        avg("Experience").alias("AverageExperience"),
        max("Experience").alias("MaxExperience"),
        min("Experience").alias("MinExperience")
      )
      .orderBy(desc("AverageExperience"))
      .show()

    // ------------------------------------------------------------
    // STEP 8: DEPARTMENT + LOCATION
    // ------------------------------------------------------------

    println("\n========== DEPARTMENT AND LOCATION ANALYSIS ==========")

    employees
      .groupBy("Department", "Location")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary"),
        avg("Salary").alias("AverageSalary")
      )
      .orderBy("Department", "Location")
      .show()

    // ------------------------------------------------------------
    // STEP 9: ROLLUP
    // ------------------------------------------------------------

    println("\n========== ROLLUP ==========")

    employees
      .rollup("Department", "Location")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("Department", "Location")
      .show()

    // ------------------------------------------------------------
    // STEP 10: CUBE
    // ------------------------------------------------------------

    println("\n========== CUBE ==========")

    employees
      .cube("Department", "Location")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("Department", "Location")
      .show()

    // ------------------------------------------------------------
    // STEP 11: FILTER DEPARTMENTS
    // ------------------------------------------------------------

    println("\n========== DEPARTMENTS WITH TOTAL SALARY > 150000 ==========")

    employees
      .groupBy("Department")
      .agg(
        sum("Salary").alias("TotalSalary"),
        count("*").alias("EmployeeCount")
      )
      .filter(col("TotalSalary") > 150000)
      .orderBy(desc("TotalSalary"))
      .show()

    // ------------------------------------------------------------
    // STEP 12: OVERALL COMPANY STATISTICS
    // ------------------------------------------------------------

    println("\n========== OVERALL COMPANY STATISTICS ==========")

    employees
      .agg(
        count("*").alias("TotalEmployees"),
        sum("Salary").alias("TotalPayroll"),
        avg("Salary").alias("AverageSalary"),
        max("Salary").alias("HighestSalary"),
        min("Salary").alias("LowestSalary"),
        avg("Experience").alias("AverageExperience")
      )
      .show()

    spark.stop()
  }
}
