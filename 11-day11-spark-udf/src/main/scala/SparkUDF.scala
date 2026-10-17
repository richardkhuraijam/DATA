import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark UDF")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ------------------------------------------------
    // 1. Read CSV
    // ------------------------------------------------

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n========== EMPLOYEE DATA ==========")
    employees.show()

    // ------------------------------------------------
    // 2. Basic UDF - Salary Category
    // ------------------------------------------------

    val salaryCategory = udf((salary: Double) => {

      if (salary >= 65000)
        "High"
      else if (salary >= 55000)
        "Medium"
      else
        "Low"

    })

    val salaryResult = employees
      .withColumn(
        "SalaryCategory",
        salaryCategory(col("Salary"))
      )

    println("\n========== SALARY CATEGORY UDF ==========")

    salaryResult
      .select(
        "Name",
        "Salary",
        "SalaryCategory"
      )
      .show()

    // ------------------------------------------------
    // 3. Experience Level UDF
    // ------------------------------------------------

    val experienceLevel = udf((experience: Int) => {

      if (experience >= 7)
        "Senior"
      else if (experience >= 4)
        "Mid-Level"
      else
        "Junior"

    })

    val experienceResult = salaryResult
      .withColumn(
        "ExperienceLevel",
        experienceLevel(col("Experience"))
      )

    println("\n========== EXPERIENCE LEVEL UDF ==========")

    experienceResult
      .select(
        "Name",
        "Experience",
        "ExperienceLevel"
      )
      .show()

    // ------------------------------------------------
    // 4. Multiple-column UDF
    // ------------------------------------------------

    val employeeDescription = udf(
      (name: String, department: String, experience: Int) => {

        s"$name works in $department with $experience years experience"

      }
    )

    val descriptionResult = experienceResult
      .withColumn(
        "Description",
        employeeDescription(
          col("Name"),
          col("Department"),
          col("Experience")
        )
      )

    println("\n========== MULTI-COLUMN UDF ==========")

    descriptionResult
      .select(
        "Name",
        "Department",
        "Description"
      )
      .show(false)

    // ------------------------------------------------
    // 5. Salary Bonus UDF
    // ------------------------------------------------

    val calculateBonus = udf((salary: Double) => {

      salary * 0.10

    })

    val bonusResult = descriptionResult
      .withColumn(
        "Bonus",
        calculateBonus(col("Salary"))
      )

    println("\n========== BONUS UDF ==========")

    bonusResult
      .select(
        "Name",
        "Salary",
        "Bonus"
      )
      .show()

    // ------------------------------------------------
    // 6. Combine Salary + Experience
    // ------------------------------------------------

    val employeeGrade = udf(
      (salary: Double, experience: Int) => {

        if (salary >= 65000 && experience >= 7)
          "A"
        else if (salary >= 55000 && experience >= 4)
          "B"
        else
          "C"

      }
    )

    val gradeResult = bonusResult
      .withColumn(
        "EmployeeGrade",
        employeeGrade(
          col("Salary"),
          col("Experience")
        )
      )

    println("\n========== EMPLOYEE GRADE UDF ==========")

    gradeResult
      .select(
        "Name",
        "Salary",
        "Experience",
        "EmployeeGrade"
      )
      .show()

    // ------------------------------------------------
    // 7. Register UDF for Spark SQL
    // ------------------------------------------------

    spark.udf.register(
      "salaryCategorySQL",
      (salary: Double) => {

        if (salary >= 65000)
          "High"
        else if (salary >= 55000)
          "Medium"
        else
          "Low"
      }
    )

    gradeResult.createOrReplaceTempView("employees")

    println("\n========== SPARK SQL UDF ==========")

    spark.sql("""
      SELECT
        Name,
        Salary,
        salaryCategorySQL(Salary) AS SalaryCategory
      FROM employees
    """).show()

    // ------------------------------------------------
    // 8. UDF + GROUP BY
    // ------------------------------------------------

    println("\n========== UDF + GROUP BY ==========")

    gradeResult
      .groupBy("EmployeeGrade")
      .agg(
        count("*").alias("EmployeeCount"),
        round(avg("Salary"), 2).alias("AverageSalary")
      )
      .orderBy("EmployeeGrade")
      .show()

    // ------------------------------------------------
    // Stop Spark
    // ------------------------------------------------

    spark.stop()
  }
}
