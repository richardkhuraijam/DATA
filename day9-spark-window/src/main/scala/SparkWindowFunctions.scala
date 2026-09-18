import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkWindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 9 - Spark Window Functions")
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
    // STEP 2: ROW_NUMBER
    // ------------------------------------------------------------

    println("\n========== ROW NUMBER BY DEPARTMENT ==========")

    val departmentWindow = Window
      .partitionBy("Department")
      .orderBy(desc("Salary"))

    employees
      .withColumn(
        "RowNumber",
        row_number().over(departmentWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "RowNumber"
      )
      .orderBy("Department", "RowNumber")
      .show()

    // ------------------------------------------------------------
    // STEP 3: RANK
    // ------------------------------------------------------------

    println("\n========== RANK BY DEPARTMENT ==========")

    employees
      .withColumn(
        "Rank",
        rank().over(departmentWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "Rank"
      )
      .orderBy("Department", "Rank")
      .show()

    // ------------------------------------------------------------
    // STEP 4: DENSE RANK
    // ------------------------------------------------------------

    println("\n========== DENSE RANK BY DEPARTMENT ==========")

    employees
      .withColumn(
        "DenseRank",
        dense_rank().over(departmentWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "DenseRank"
      )
      .orderBy("Department", "DenseRank")
      .show()

    // ------------------------------------------------------------
    // STEP 5: TOP EMPLOYEE IN EACH DEPARTMENT
    // ------------------------------------------------------------

    println("\n========== TOP EMPLOYEE IN EACH DEPARTMENT ==========")

    employees
      .withColumn(
        "Rank",
        row_number().over(departmentWindow)
      )
      .filter(col("Rank") === 1)
      .select(
        "Name",
        "Department",
        "Salary"
      )
      .orderBy("Department")
      .show()

    // ------------------------------------------------------------
    // STEP 6: DEPARTMENT AVERAGE SALARY
    // ------------------------------------------------------------

    println("\n========== SALARY VS DEPARTMENT AVERAGE ==========")

    val salaryWindow = Window
      .partitionBy("Department")

    employees
      .withColumn(
        "DepartmentAverage",
        avg("Salary").over(salaryWindow)
      )
      .withColumn(
        "DifferenceFromAverage",
        col("Salary") - col("DepartmentAverage")
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "DepartmentAverage",
        "DifferenceFromAverage"
      )
      .orderBy(col("Department"), desc("Salary"))
      .show()

    // ------------------------------------------------------------
    // STEP 7: LAG
    // ------------------------------------------------------------

    println("\n========== LAG - PREVIOUS EMPLOYEE SALARY ==========")

    val salaryOrderWindow = Window
      .partitionBy("Department")
      .orderBy("Salary")

    employees
      .withColumn(
        "PreviousSalary",
        lag("Salary", 1).over(salaryOrderWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "PreviousSalary"
      )
      .orderBy("Department", "Salary")
      .show()

    // ------------------------------------------------------------
    // STEP 8: LEAD
    // ------------------------------------------------------------

    println("\n========== LEAD - NEXT EMPLOYEE SALARY ==========")

    employees
      .withColumn(
        "NextSalary",
        lead("Salary", 1).over(salaryOrderWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "NextSalary"
      )
      .orderBy("Department", "Salary")
      .show()

    // ------------------------------------------------------------
    // STEP 9: RUNNING TOTAL
    // ------------------------------------------------------------

    println("\n========== RUNNING SALARY TOTAL BY DEPARTMENT ==========")

    val runningWindow = Window
      .partitionBy("Department")
      .orderBy("Salary")
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    employees
      .withColumn(
        "RunningTotal",
        sum("Salary").over(runningWindow)
      )
      .select(
        "Name",
        "Department",
        "Salary",
        "RunningTotal"
      )
      .orderBy("Department", "Salary")
      .show()

    // ------------------------------------------------------------
    // STEP 10: EXPERIENCE RANKING
    // ------------------------------------------------------------

    println("\n========== EXPERIENCE RANKING ==========")

    val experienceWindow = Window
      .partitionBy("Department")
      .orderBy(desc("Experience"))

    employees
      .withColumn(
        "ExperienceRank",
        dense_rank().over(experienceWindow)
      )
      .select(
        "Name",
        "Department",
        "Experience",
        "ExperienceRank"
      )
      .orderBy("Department", "ExperienceRank")
      .show()

    spark.stop()
  }
}
