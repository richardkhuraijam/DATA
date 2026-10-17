import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object SparkCache {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Cache and Persistence")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------
    // 1. Read data
    // ------------------------------------------------

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n========== ORIGINAL DATA ==========")
    employees.show()

    // ------------------------------------------------
    // 2. Create a filtered DataFrame
    // ------------------------------------------------

    val highSalaryEmployees = employees
      .filter(col("Salary") >= 55000)

    println("\n========== HIGH SALARY EMPLOYEES ==========")
    highSalaryEmployees.show()

    // ------------------------------------------------
    // 3. Cache the DataFrame
    // ------------------------------------------------

    highSalaryEmployees.cache()

    println("\n========== AFTER CACHE ==========")
    println("DataFrame has been marked for caching.")

    // ------------------------------------------------
    // 4. First action
    // ------------------------------------------------

    println("\n========== FIRST ACTION ==========")

    val employeeCount = highSalaryEmployees.count()

    println("High salary employee count: " + employeeCount)

    // ------------------------------------------------
    // 5. Reuse cached DataFrame
    // ------------------------------------------------

    println("\n========== SECOND ACTION ==========")

    highSalaryEmployees
      .groupBy("Department")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2).alias("AverageSalary")
      )
      .orderBy("Department")
      .show()

    // ------------------------------------------------
    // 6. Another action on cached data
    // ------------------------------------------------

    println("\n========== THIRD ACTION ==========")

    highSalaryEmployees
      .groupBy("Location")
      .agg(
        count("*").alias("EmployeeCount"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("Location")
      .show()

    // ------------------------------------------------
    // 7. Explain execution plan
    // ------------------------------------------------

    println("\n========== EXECUTION PLAN ==========")

    highSalaryEmployees
      .groupBy("Department")
      .agg(avg("Salary"))
      .explain()

    // ------------------------------------------------
    // 8. Unpersist
    // ------------------------------------------------

    println("\n========== UNPERSIST ==========")

    highSalaryEmployees.unpersist()

    println("DataFrame has been unpersisted.")

    // ------------------------------------------------
    // 9. Persist with MEMORY_AND_DISK
    // ------------------------------------------------

    println("\n========== PERSIST MEMORY_AND_DISK ==========")

    val persistedEmployees = employees
      .filter(col("Experience") >= 4)
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("DataFrame persisted using MEMORY_AND_DISK.")

    // ------------------------------------------------
    // 10. Use persisted DataFrame
    // ------------------------------------------------

    println("\n========== PERSISTED DATA ==========")

    persistedEmployees.show()

    println("\n========== PERSISTED DATA ANALYSIS ==========")

    persistedEmployees
      .groupBy("Department")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2).alias("AverageSalary"),
        max("Salary").alias("HighestSalary")
      )
      .orderBy("Department")
      .show()

    // ------------------------------------------------
    // 11. Remove persisted data
    // ------------------------------------------------

    persistedEmployees.unpersist()

    println("Persisted DataFrame has been unpersisted.")

    // ------------------------------------------------
    // Stop Spark
    // ------------------------------------------------

    spark.stop()
  }
}
