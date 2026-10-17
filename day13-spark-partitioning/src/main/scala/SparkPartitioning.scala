import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPartitioning {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Partitioning")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Required for converting Seq to DataFrame
    import spark.implicits._

    println("\n========================================")
    println("       SPARK PARTITIONING")
    println("========================================")

    // ------------------------------------------------
    // 1. Create sample data
    // ------------------------------------------------

    val data = Seq(
      ("Alice", "IT", 45000),
      ("Bob", "Finance", 55000),
      ("Charlie", "IT", 60000),
      ("David", "HR", 50000),
      ("Eva", "Marketing", 65000),
      ("Frank", "Finance", 58000),
      ("Grace", "IT", 60000),
      ("Helen", "HR", 52000),
      ("Ian", "Finance", 58000),
      ("Jack", "Marketing", 70000),
      ("Karen", "IT", 72000),
      ("Liam", "HR", 48000)
    )

    val employees = data.toDF(
      "Name",
      "Department",
      "Salary"
    )

    println("\n========== ORIGINAL DATA ==========")
    employees.show()

    // ------------------------------------------------
    // 2. Original number of partitions
    // ------------------------------------------------

    println("\n========== ORIGINAL PARTITIONS ==========")

    println(
      "Number of partitions: " +
        employees.rdd.getNumPartitions
    )

    // ------------------------------------------------
    // 3. Repartition
    // ------------------------------------------------

    println("\n========== REPARTITION TO 4 ==========")

    val repartitioned = employees.repartition(4)

    println(
      "Number of partitions after repartition: " +
        repartitioned.rdd.getNumPartitions
    )

    repartitioned.show()

    // ------------------------------------------------
    // 4. Repartition by Department
    // ------------------------------------------------

    println("\n========== REPARTITION BY DEPARTMENT ==========")

    val departmentPartitioned =
      employees.repartition(4, col("Department"))

    println(
      "Number of partitions: " +
        departmentPartitioned.rdd.getNumPartitions
    )

    departmentPartitioned.show()

    // ------------------------------------------------
    // 5. Coalesce
    // ------------------------------------------------

    println("\n========== COALESCE TO 2 ==========")

    val coalesced =
      repartitioned.coalesce(2)

    println(
      "Number of partitions after coalesce: " +
        coalesced.rdd.getNumPartitions
    )

    coalesced.show()

    // ------------------------------------------------
    // 6. mapPartitions
    // ------------------------------------------------

    println("\n========== MAP PARTITIONS ==========")

    val partitionData =
      employees.rdd.mapPartitions { partition =>

        partition.map { row =>

          val name = row.getAs[String]("Name")
          val salary = row.getAs[Int]("Salary")

          name + " -> Salary: " + salary
        }
      }

    partitionData.collect().foreach(println)

    // ------------------------------------------------
    // 7. Partition information
    // ------------------------------------------------

    println("\n========== PARTITION INFORMATION ==========")

    val partitionInfo =
      employees.rdd.mapPartitionsWithIndex {
        (partitionIndex, iterator) =>

          val rows = iterator.toList

          Iterator(
            "Partition " +
              partitionIndex +
              " contains " +
              rows.size +
              " records"
          )
      }

    partitionInfo.collect().foreach(println)

    // ------------------------------------------------
    // 8. Salary analysis
    // ------------------------------------------------

    println("\n========== SALARY BY DEPARTMENT ==========")

    employees
      .groupBy("Department")
      .agg(
        count("*").alias("EmployeeCount"),
        avg("Salary").alias("AverageSalary"),
        sum("Salary").alias("TotalSalary")
      )
      .orderBy("Department")
      .show()

    // ------------------------------------------------
    // 9. Write partitioned output
    // ------------------------------------------------

    println("\n========== WRITING PARTITIONED DATA ==========")

    employees
      .write
      .mode("overwrite")
      .partitionBy("Department")
      .option("header", "true")
      .csv("output/employees_by_department")

    println(
      "Data written to: output/employees_by_department"
    )

    // ------------------------------------------------
    // 10. Summary
    // ------------------------------------------------

    println("\n========================================")
    println("       PARTITIONING SUMMARY")
    println("========================================")

    println(
      "Original partitions: " +
        employees.rdd.getNumPartitions
    )

    println(
      "Repartitioned partitions: " +
        repartitioned.rdd.getNumPartitions
    )

    println(
      "Coalesced partitions: " +
        coalesced.rdd.getNumPartitions
    )

    println("\n========== PROGRAM COMPLETED ==========")

    spark.stop()
  }
}
