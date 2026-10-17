import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinOptimization {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Spark Join Optimization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println()
    println("======================================================")
    println("             SPARK JOIN OPTIMIZATION")
    println("======================================================")

    // ============================================================
    // EMPLOYEE DATA
    // ============================================================

    val employees = (1 to 100000).map { id =>
      (
        id,
        s"Employee_$id",
        (id % 5) + 1,
        30000 + (id % 50000)
      )
    }.toDF(
      "EmployeeID",
      "Name",
      "DepartmentID",
      "Salary"
    )

    // ============================================================
    // DEPARTMENT DATA
    // ============================================================

    val departments = Seq(
      (1, "IT"),
      (2, "Finance"),
      (3, "HR"),
      (4, "Marketing"),
      (5, "Operations")
    ).toDF(
      "DepartmentID",
      "DepartmentName"
    )

    // ============================================================
    // DATA INFORMATION
    // ============================================================

    println()
    println("========== DATA INFORMATION ==========")

    println(
      s"Employee records: ${employees.count()}"
    )

    println(
      s"Department records: ${departments.count()}"
    )

    // ============================================================
    // NORMAL INNER JOIN
    // ============================================================

    println()
    println("========== NORMAL INNER JOIN ==========")

    val normalJoin =
      employees.join(
        departments,
        employees("DepartmentID") ===
          departments("DepartmentID"),
        "inner"
      )

    normalJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        departments("DepartmentName"),
        employees("Salary")
      )
      .show(10)

    // ============================================================
    // NORMAL JOIN EXECUTION PLAN
    // ============================================================

    println()
    println("========== NORMAL JOIN EXECUTION PLAN ==========")

    normalJoin.explain()

    // ============================================================
    // BROADCAST JOIN
    // ============================================================

    println()
    println("========== BROADCAST JOIN ==========")

    val broadcastJoin =
      employees.join(
        broadcast(departments),
        employees("DepartmentID") ===
          departments("DepartmentID"),
        "inner"
      )

    broadcastJoin
      .select(
        employees("EmployeeID"),
        employees("Name"),
        departments("DepartmentName"),
        employees("Salary")
      )
      .show(10)

    // ============================================================
    // BROADCAST JOIN EXECUTION PLAN
    // ============================================================

    println()
    println("========== BROADCAST JOIN EXECUTION PLAN ==========")

    broadcastJoin.explain()

    // ============================================================
    // DEPARTMENT SALARY ANALYSIS
    // ============================================================

    println()
    println("========== DEPARTMENT SALARY ANALYSIS ==========")

    broadcastJoin
      .groupBy("DepartmentName")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2).alias("AverageSalary"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("DepartmentName")
      .show()

    // ============================================================
    // HIGH SALARY EMPLOYEES
    // ============================================================

    println()
    println("========== HIGH SALARY EMPLOYEES ==========")

    broadcastJoin
      .filter(col("Salary") > 60000)
      .select(
        col("EmployeeID"),
        col("Name"),
        col("DepartmentName"),
        col("Salary")
      )
      .show(10)

    // ============================================================
    // CACHE JOIN RESULT
    // ============================================================

    println()
    println("========== CACHE JOIN RESULT ==========")

    val cachedJoin =
      broadcastJoin.cache()

    println(
      s"Cached join records: ${cachedJoin.count()}"
    )

    println()
    println("Running cached query again...")

    val cachedHighSalaryCount =
      cachedJoin
        .filter(col("Salary") > 60000)
        .count()

    println(
      s"High salary employees: $cachedHighSalaryCount"
    )

    // ============================================================
    // PARTITION INFORMATION
    // ============================================================

    println()
    println("========== PARTITION INFORMATION ==========")

    println(
      s"Employee partitions: ${employees.rdd.getNumPartitions}"
    )

    println(
      s"Department partitions: ${departments.rdd.getNumPartitions}"
    )

    println(
      s"Joined partitions: ${broadcastJoin.rdd.getNumPartitions}"
    )

    // ============================================================
    // UNPERSIST
    // ============================================================

    println()
    println("========== UNPERSIST ==========")

    cachedJoin.unpersist()

    println(
      "Cached join DataFrame released."
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
