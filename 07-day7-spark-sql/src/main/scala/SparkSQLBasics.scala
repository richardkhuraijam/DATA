import org.apache.spark.sql.SparkSession

object SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    // ================================================
    // CREATE SPARK SESSION
    // ================================================

    val spark = SparkSession.builder()
      .appName("Spark SQL Basics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("\n===== SPARK SQL ANALYSIS =====")


    // ================================================
    // READ CSV
    // ================================================

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")


    // ================================================
    // DISPLAY DATA
    // ================================================

    println("\n===== EMPLOYEE DATA =====")

    df.show()


    // ================================================
    // CREATE TEMPORARY SQL VIEW
    // ================================================

    df.createOrReplaceTempView("employees")

    println("\nTemporary view 'employees' created.")


    // ================================================
    // 1. SELECT
    // ================================================

    println("\n===== 1. SELECT NAME AND SALARY =====")

    spark.sql("""
      SELECT Name, Salary
      FROM employees
    """).show()


    // ================================================
    // 2. WHERE
    // ================================================

    println("\n===== 2. EMPLOYEES WITH SALARY > 55000 =====")

    spark.sql("""
      SELECT Name, Department, Salary
      FROM employees
      WHERE Salary > 55000
    """).show()


    // ================================================
    // 3. WHERE WITH STRING CONDITION
    // ================================================

    println("\n===== 3. IT DEPARTMENT EMPLOYEES =====")

    spark.sql("""
      SELECT Name, Salary, Experience
      FROM employees
      WHERE Department = 'IT'
    """).show()


    // ================================================
    // 4. GROUP BY + SUM
    // ================================================

    println("\n===== 4. TOTAL SALARY BY DEPARTMENT =====")

    spark.sql("""
      SELECT
        Department,
        SUM(Salary) AS Total_Salary
      FROM employees
      GROUP BY Department
    """).show()


    // ================================================
    // 5. GROUP BY + AVG
    // ================================================

    println("\n===== 5. AVERAGE SALARY BY DEPARTMENT =====")

    spark.sql("""
      SELECT
        Department,
        AVG(Salary) AS Average_Salary
      FROM employees
      GROUP BY Department
    """).show()


    // ================================================
    // 6. COUNT
    // ================================================

    println("\n===== 6. EMPLOYEE COUNT BY DEPARTMENT =====")

    spark.sql("""
      SELECT
        Department,
        COUNT(*) AS Employee_Count
      FROM employees
      GROUP BY Department
    """).show()


    // ================================================
    // 7. ORDER BY
    // ================================================

    println("\n===== 7. EMPLOYEES ORDERED BY SALARY =====")

    spark.sql("""
      SELECT Name, Department, Salary
      FROM employees
      ORDER BY Salary DESC
    """).show()


    // ================================================
    // 8. HAVING
    // ================================================

    println("\n===== 8. DEPARTMENTS WITH AVERAGE SALARY > 55000 =====")

    spark.sql("""
      SELECT
        Department,
        AVG(Salary) AS Average_Salary
      FROM employees
      GROUP BY Department
      HAVING AVG(Salary) > 55000
    """).show()


    // ================================================
    // 9. MULTIPLE CONDITIONS
    // ================================================

    println("\n===== 9. IT EMPLOYEES WITH EXPERIENCE >= 4 =====")

    spark.sql("""
      SELECT
        Name,
        Salary,
        Experience
      FROM employees
      WHERE Department = 'IT'
        AND Experience >= 4
    """).show()


    // ================================================
    // 10. MAXIMUM SALARY
    // ================================================

    println("\n===== 10. HIGHEST SALARY =====")

    spark.sql("""
      SELECT
        MAX(Salary) AS Highest_Salary
      FROM employees
    """).show()


    // ================================================
    // 11. MINIMUM SALARY
    // ================================================

    println("\n===== 11. LOWEST SALARY =====")

    spark.sql("""
      SELECT
        MIN(Salary) AS Lowest_Salary
      FROM employees
    """).show()


    // ================================================
    // 12. AVERAGE EXPERIENCE
    // ================================================

    println("\n===== 12. AVERAGE EXPERIENCE =====")

    spark.sql("""
      SELECT
        AVG(Experience) AS Average_Experience
      FROM employees
    """).show()


    // ================================================
    // PROGRAM COMPLETE
    // ================================================

    spark.stop()

    println("\n===== PROGRAM COMPLETED =====")
  }
}
