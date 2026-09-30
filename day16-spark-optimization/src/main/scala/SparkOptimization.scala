import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object SparkOptimization {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Spark Performance Optimization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println()
    println("======================================================")
    println("        SPARK PERFORMANCE OPTIMIZATION")
    println("======================================================")

    // ============================================================
    // CREATE LARGE DATASET
    // ============================================================

    val data = (1 to 1000000).map { id =>
      (
        id,
        s"Employee_$id",
        id % 5,
        30000 + (id % 50000)
      )
    }

    val employees = data.toDF(
      "EmployeeID",
      "Name",
      "DepartmentID",
      "Salary"
    )

    // ============================================================
    // ORIGINAL DATA
    // ============================================================

    println()
    println("========== ORIGINAL DATA ==========")

    val totalEmployees = employees.count()

    println(
      s"Number of records: $totalEmployees"
    )

    // ============================================================
    // ORIGINAL PARTITIONS
    // ============================================================

    println()
    println("========== ORIGINAL PARTITIONS ==========")

    println(
      s"Number of partitions: ${employees.rdd.getNumPartitions}"
    )

    // ============================================================
    // REPARTITION
    // ============================================================

    println()
    println("========== REPARTITION ==========")

    val repartitionedEmployees =
      employees.repartition(8)

    println(
      s"Partitions after repartition: " +
        s"${repartitionedEmployees.rdd.getNumPartitions}"
    )

    // ============================================================
    // REPARTITION BY DEPARTMENT
    // ============================================================

    println()
    println("========== REPARTITION BY DEPARTMENT ==========")

    val departmentPartitioned =
      employees.repartition(
        8,
        col("DepartmentID")
      )

    println(
      s"Partitions after department repartition: " +
        s"${departmentPartitioned.rdd.getNumPartitions}"
    )

    // ============================================================
    // COALESCE
    // ============================================================

    println()
    println("========== COALESCE ==========")

    val coalescedEmployees =
      repartitionedEmployees.coalesce(4)

    println(
      s"Partitions after coalesce: " +
        s"${coalescedEmployees.rdd.getNumPartitions}"
    )

    // ============================================================
    // CACHE
    // ============================================================

    println()
    println("========== CACHE ==========")

    val highSalaryEmployees =
      employees
        .filter(col("Salary") > 60000)
        .cache()

    val cachedCount =
      highSalaryEmployees.count()

    println(
      s"Cached records: $cachedCount"
    )

    println("DataFrame cached successfully.")

    // ============================================================
    // CACHE REUSE
    // ============================================================

    println()
    println("========== CACHE REUSE ==========")

    val highSalaryCount =
      highSalaryEmployees.count()

    val averageHighSalary =
      highSalaryEmployees
        .agg(
          round(
            avg("Salary"),
            2
          ).alias("AverageSalary")
        )
        .collect()(0)
        .getAs[Double]("AverageSalary")

    println(
      s"High salary employee count: $highSalaryCount"
    )

    println(
      s"Average high salary: $averageHighSalary"
    )

    // ============================================================
    // PERSIST
    // ============================================================

    println()
    println("========== PERSIST ==========")

    val persistedEmployees =
      employees
        .filter(col("Salary") >= 50000)
        .persist(
          StorageLevel.MEMORY_AND_DISK
        )

    val persistedCount =
      persistedEmployees.count()

    println(
      s"Persisted records: $persistedCount"
    )

    println(
      s"Storage level: ${persistedEmployees.storageLevel}"
    )

    // ============================================================
    // DEPARTMENT ANALYSIS
    // ============================================================

    println()
    println("========== DEPARTMENT ANALYSIS ==========")

    departmentPartitioned
      .groupBy("DepartmentID")
      .agg(
        count("*").alias("EmployeeCount"),
        round(
          avg("Salary"),
          2
        ).alias("AverageSalary"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("DepartmentID")
      .show()

    // ============================================================
    // EXECUTION PLAN
    // ============================================================

    println()
    println("========== EXECUTION PLAN ==========")

    highSalaryEmployees
      .groupBy("DepartmentID")
      .agg(
        avg("Salary")
          .alias("AverageSalary")
      )
      .explain()

    // ============================================================
    // UNPERSIST
    // ============================================================

    println()
    println("========== UNPERSIST ==========")

    highSalaryEmployees.unpersist()

    persistedEmployees.unpersist()

    println(
      "Cached and persisted DataFrames released."
    )

    // ============================================================
    // PROGRAM COMPLETED
    // ============================================================

    println()
    println("======================================================")
    println("              PROGRAM COMPLETED")
    println("======================================================")

    spark.stop()
  }
}
