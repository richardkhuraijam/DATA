import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataSkew {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // CREATE SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Spark Data Skew and Salting")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println()
    println("======================================================")
    println("             SPARK DATA SKEW & SALTING")
    println("======================================================")

    // ============================================================
    // CREATE SKEWED DATA
    // ============================================================

    println()
    println("========== CREATING SKEWED DATA ==========")

    val data = (1 to 100000).map { id =>

      val department =
        if (id <= 80000) {
          "IT"
        } else if (id <= 90000) {
          "Finance"
        } else if (id <= 95000) {
          "HR"
        } else {
          "Marketing"
        }

      (
        id,
        department,
        30000 + (id % 50000)
      )
    }

    val employees = data.toDF(
      "EmployeeID",
      "Department",
      "Salary"
    )

    println(
      s"Total records: ${employees.count()}"
    )

    // ============================================================
    // SHOW SKEWED DISTRIBUTION
    // ============================================================

    println()
    println("========== SKEWED DATA DISTRIBUTION ==========")

    employees
      .groupBy("Department")
      .count()
      .orderBy(desc("count"))
      .show()

    // ============================================================
    // REPARTITION BY SKEWED COLUMN
    // ============================================================

    println()
    println("========== REPARTITION BY DEPARTMENT ==========")

    val skewedData =
      employees.repartition(
        8,
        col("Department")
      )

    println(
      s"Number of partitions: " +
        s"${skewedData.rdd.getNumPartitions}"
    )

    // ============================================================
    // PARTITION SIZE BEFORE SALTING
    // ============================================================

    println()
    println("========== PARTITION DISTRIBUTION BEFORE SALTING ==========")

    val beforeSalting =
      skewedData.rdd
        .mapPartitions { rows =>
          Iterator(rows.size)
        }
        .collect()

    beforeSalting.zipWithIndex.foreach {
      case (size, index) =>
        println(
          s"Partition $index -> $size records"
        )
    }

    // ============================================================
    // SALTING
    // ============================================================

    println()
    println("========== APPLYING SALTING ==========")

    val saltedData =
      employees.withColumn(
        "Salt",
        when(
          col("Department") === "IT",
          pmod(
            col("EmployeeID"),
            lit(8)
          )
        ).otherwise(
          lit(0)
        )
      )

    println()
    println("Salted data sample:")

    saltedData
      .select(
        "EmployeeID",
        "Department",
        "Salary",
        "Salt"
      )
      .show(10)

    // ============================================================
    // REPARTITION USING DEPARTMENT + SALT
    // ============================================================

    println()
    println("========== REPARTITION USING DEPARTMENT + SALT ==========")

    val saltedPartitioned =
      saltedData.repartition(
        8,
        col("Department"),
        col("Salt")
      )

    println(
      s"Number of partitions after salting: " +
        s"${saltedPartitioned.rdd.getNumPartitions}"
    )

    // ============================================================
    // PARTITION SIZE AFTER SALTING
    // ============================================================

    println()
    println("========== PARTITION DISTRIBUTION AFTER SALTING ==========")

    val afterSalting =
      saltedPartitioned.rdd
        .mapPartitions { rows =>
          Iterator(rows.size)
        }
        .collect()

    afterSalting.zipWithIndex.foreach {
      case (size, index) =>
        println(
          s"Partition $index -> $size records"
        )
    }

    // ============================================================
    // SALTED DISTRIBUTION
    // ============================================================

    println()
    println("========== SALT DISTRIBUTION ==========")

    saltedData
      .groupBy(
        "Department",
        "Salt"
      )
      .count()
      .orderBy(
        "Department",
        "Salt"
      )
      .show(30)

    // ============================================================
    // NORMAL AGGREGATION
    // ============================================================

    println()
    println("========== NORMAL AGGREGATION ==========")

    employees
      .groupBy("Department")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2)
          .alias("AverageSalary"),
        sum("Salary")
          .alias("TotalSalary")
      )
      .orderBy("Department")
      .show()

    // ============================================================
    // SALTED AGGREGATION
    // ============================================================

    println()
    println("========== SALTED AGGREGATION ==========")

    val partialAggregation =
      saltedData
        .groupBy(
          "Department",
          "Salt"
        )
        .agg(
          count("*").alias("EmployeeCount"),
          sum("Salary").alias("TotalSalary")
        )

    println("Partial aggregation:")

    partialAggregation
      .orderBy(
        "Department",
        "Salt"
      )
      .show(30)

    // ============================================================
    // FINAL AGGREGATION
    // ============================================================

    println()
    println("========== FINAL AGGREGATION ==========")

    partialAggregation
      .groupBy("Department")
      .agg(
        sum("EmployeeCount")
          .alias("EmployeeCount"),
        sum("TotalSalary")
          .alias("TotalSalary")
      )
      .withColumn(
        "AverageSalary",
        round(
          col("TotalSalary") /
            col("EmployeeCount"),
          2
        )
      )
      .orderBy("Department")
      .show()

    // ============================================================
    // EXECUTION PLAN
    // ============================================================

    println()
    println("========== EXECUTION PLAN ==========")

    saltedPartitioned
      .groupBy(
        "Department",
        "Salt"
      )
      .count()
      .explain()

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
