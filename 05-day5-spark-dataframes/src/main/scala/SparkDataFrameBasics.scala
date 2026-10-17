import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataFrameBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark DataFrame Basics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Create sample data
    val employees = Seq(
      ("Richard", "IT", 50000),
      ("Alice", "HR", 45000),
      ("Bob", "IT", 60000),
      ("John", "Finance", 55000),
      ("Emma", "HR", 48000),
      ("David", "IT", 70000)
    )

    // Create DataFrame
    val df = employees.toDF("Name", "Department", "Salary")

    println("\n===== ORIGINAL DATA =====")
    df.show()

    // Select specific columns
    println("\n===== SELECT NAME AND SALARY =====")
    df.select("Name", "Salary").show()

    // Filter employees
    println("\n===== EMPLOYEES WITH SALARY > 50000 =====")
    df.filter(col("Salary") > 50000).show()

    // Add a new column
    println("\n===== SALARY WITH BONUS =====")
    df.withColumn("Bonus", col("Salary") * 0.10).show()

    // GroupBy and aggregation
    println("\n===== AVERAGE SALARY BY DEPARTMENT =====")
    df.groupBy("Department")
      .agg(avg("Salary").alias("Average Salary"))
      .show()

    // Count employees by department
    println("\n===== EMPLOYEE COUNT BY DEPARTMENT =====")
    df.groupBy("Department")
      .count()
      .show()

    // Order by salary
    println("\n===== EMPLOYEES ORDERED BY SALARY =====")
    df.orderBy(col("Salary").desc).show()

    spark.stop()
  }
}
