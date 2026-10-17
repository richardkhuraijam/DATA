import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFPerformance {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Spark UDF Performance")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println()
    println("======================================================")
    println("              SPARK UDF PERFORMANCE")
    println("======================================================")

    // ============================================================
    // CREATE EMPLOYEE DATA
    // ============================================================

    val employees = (1 to 20000).map { id =>
      (
        id,
        s"Employee_$id",
        30000 + (id % 50000),
        (id % 5) + 1
      )
    }.toDF(
      "EmployeeID",
      "Name",
      "Salary",
      "DepartmentID"
    )

    println()
    println("========== EMPLOYEE DATA ==========")

    println(
      s"Total employees: ${employees.count()}"
    )

    employees.show(10)

    // ============================================================
    // BUILT-IN SPARK FUNCTION
    // ============================================================

    println()
    println("========== BUILT-IN FUNCTION ==========")

    val builtInResult =
      employees.withColumn(
        "SalaryCategory",
        when(
          col("Salary") >= 65000,
          "High"
        ).when(
          col("Salary") >= 50000,
          "Medium"
        ).otherwise(
          "Low"
        )
      )

    builtInResult
      .select(
        "EmployeeID",
        "Salary",
        "SalaryCategory"
      )
      .show(10)

    // ============================================================
    // BUILT-IN FUNCTION AGGREGATION
    // ============================================================

    println()
    println("========== BUILT-IN FUNCTION AGGREGATION ==========")

    builtInResult
      .groupBy("SalaryCategory")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2)
          .alias("AverageSalary")
      )
      .orderBy("SalaryCategory")
      .show()

    // ============================================================
    // DEFINE SCALA UDF
    // ============================================================

    println()
    println("========== SCALA UDF ==========")

    val salaryCategoryUDF =
      udf { salary: Int =>
        if (salary >= 65000) {
          "High"
        } else if (salary >= 50000) {
          "Medium"
        } else {
          "Low"
        }
      }

    // ============================================================
    // APPLY UDF
    // ============================================================

    val udfResult =
      employees.withColumn(
        "SalaryCategory",
        salaryCategoryUDF(col("Salary"))
      )

    udfResult
      .select(
        "EmployeeID",
        "Salary",
        "SalaryCategory"
      )
      .show(10)

    // ============================================================
    // UDF AGGREGATION
    // ============================================================

    println()
    println("========== UDF AGGREGATION ==========")

    udfResult
      .groupBy("SalaryCategory")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2)
          .alias("AverageSalary")
      )
      .orderBy("SalaryCategory")
      .show()

    // ============================================================
    // UDF WITH MULTIPLE CONDITIONS
    // ============================================================

    println()
    println("========== UDF WITH MULTIPLE CONDITIONS ==========")

    val experienceUDF =
      udf { departmentId: Int =>
        departmentId match {
          case 1 => "Technology"
          case 2 => "Finance"
          case 3 => "Human Resources"
          case 4 => "Marketing"
          case 5 => "Operations"
          case _ => "Unknown"
        }
      }

    val departmentResult =
      udfResult.withColumn(
        "DepartmentName",
        experienceUDF(col("DepartmentID"))
      )

    departmentResult
      .select(
        "EmployeeID",
        "DepartmentID",
        "DepartmentName",
        "Salary",
        "SalaryCategory"
      )
      .show(10)

    // ============================================================
    // UDF EXECUTION PLAN
    // ============================================================

    println()
    println("========== UDF EXECUTION PLAN ==========")

    udfResult
      .filter(
        col("SalaryCategory") === "High"
      )
      .explain()

    // ============================================================
    // BUILT-IN EXECUTION PLAN
    // ============================================================

    println()
    println("========== BUILT-IN EXECUTION PLAN ==========")

    builtInResult
      .filter(
        col("SalaryCategory") === "High"
      )
      .explain()

    // ============================================================
    // PERFORMANCE MESSAGE
    // ============================================================

    println()
    println("========== PERFORMANCE NOTE ==========")

    println(
      "Built-in Spark functions are generally preferred " +
      "because Spark can optimize them using Catalyst."
    )

    println(
      "Scala UDFs are useful when custom logic cannot " +
      "be easily expressed using Spark built-in functions."
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
