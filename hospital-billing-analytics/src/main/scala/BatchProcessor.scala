
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object BatchProcessor {

  // ======================================
  // UDF - BILLING STATUS CLASSIFICATION
  // ======================================

  val classifyBillingStatus = udf((outstanding: Double) => {
    if (outstanding == 0.0) "PAID"
    else "DUE"
  })

  def main(args: Array[String]): Unit = {

    // ======================================
    // SPARK SESSION
    // ======================================

    val spark = SparkSession.builder()
      .appName("Hospital Billing and Revenue Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println("Hospital Billing Analytics Started!")
    println("======================================")

    // ======================================
    // 1. READ INPUT DATA
    // ======================================

    val visitsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/visits.csv")

    val proceduresDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/procedures.csv")

    val insuranceDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/insurance.csv")

    val paymentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/payments.csv")

    println("\n--- INPUT DATA ---")

    println("\nVisits:")
    visitsDF.show()

    println("\nProcedures:")
    proceduresDF.show()

    println("\nInsurance:")
    insuranceDF.show()

    println("\nPayments:")
    paymentsDF.show()

    // ======================================
    // 2. BROADCAST VARIABLE
    // ======================================

    val insuranceBroadcast =
      spark.sparkContext.broadcast(
        insuranceDF.collect()
      )

    println("\n--- BROADCAST VARIABLE ---")
    println(
      s"Broadcasted insurance records: ${insuranceBroadcast.value.length}"
    )

    // ======================================
    // 3. JOIN VISITS + PROCEDURES
    // ======================================

    val visitProcedureDF = visitsDF
      .join(
        proceduresDF,
        visitsDF("visit_id") === proceduresDF("visit_id"),
        "inner"
      )

    println("\n--- VISIT + PROCEDURE JOIN ---")
    visitProcedureDF.show()

    // ======================================
    // 4. TOTAL BILL PER VISIT
    // ======================================

    val billDF = visitProcedureDF
      .groupBy(
        visitsDF("visit_id"),
        visitsDF("patient_id"),
        visitsDF("department")
      )
      .agg(
        sum(proceduresDF("procedure_cost")).alias("total_bill")
      )

    println("\n--- TOTAL BILL PER VISIT ---")
    billDF.show()

    // ======================================
    // 5. JOIN INSURANCE
    // ======================================

    val insuranceBillDF = billDF
      .join(
        insuranceDF,
        billDF("patient_id") === insuranceDF("patient_id"),
        "left"
      )
      .select(
        billDF("visit_id"),
        billDF("patient_id"),
        billDF("department"),
        billDF("total_bill"),
        coalesce(
          insuranceDF("coverage_percent"),
          lit(0.0)
        ).alias("coverage_percent")
      )

    println("\n--- INSURANCE JOIN ---")
    insuranceBillDF.show()

    // ======================================
    // 6. CALCULATE INSURANCE COVERAGE
    // ======================================

    val coverageDF = insuranceBillDF
      .withColumn(
        "insurance_covered",
        col("total_bill") * col("coverage_percent") / 100
      )

    println("\n--- INSURANCE COVERAGE ---")
    coverageDF.show()

    // ======================================
    // 7. AGGREGATE PAYMENTS
    // ======================================

    val paymentsAggDF = paymentsDF
      .groupBy("visit_id")
      .agg(
        sum("payment_amount").alias("amount_paid")
      )

    println("\n--- PAYMENT AGGREGATION ---")
    paymentsAggDF.show()

    // ======================================
    // 8. JOIN PAYMENTS
    // ======================================

    val billingWithPaymentsDF = coverageDF
      .join(
        paymentsAggDF,
        Seq("visit_id"),
        "left"
      )
      .withColumn(
        "amount_paid",
        coalesce(col("amount_paid"), lit(0.0))
      )

    // ======================================
    // 9. OUTSTANDING BALANCE
    // ======================================

    val billingRecordsDF = billingWithPaymentsDF
      .withColumn(
        "outstanding_balance",
        greatest(
          lit(0.0),
          col("total_bill") -
            col("insurance_covered") -
            col("amount_paid")
        )
      )
      .withColumn(
        "billing_status",
        when(
          col("outstanding_balance") === 0.0,
          "PAID"
        ).otherwise("OUTSTANDING")
      )
      .select(
        "visit_id",
        "patient_id",
        "department",
        "total_bill",
        "insurance_covered",
        "amount_paid",
        "outstanding_balance",
        "billing_status"
      )

    // ======================================
    // 10. CACHE
    // ======================================

    billingRecordsDF.cache()

    billingRecordsDF.count()
    
       // ======================================
    // 18. PERSIST
    // ======================================

    println("\n======================================")
    println("STEP 42 - PERSIST")
    println("======================================")

    val persistedBillingDF =
      billingRecordsDF.persist(StorageLevel.MEMORY_AND_DISK)

    // Trigger computation so Spark materializes the persisted data
    persistedBillingDF.count()

    println(
      s"Persist storage level: ${persistedBillingDF.storageLevel}"
    )

    println("Billing records persisted using MEMORY_AND_DISK")

    println("\n--- FINAL BILLING RECORDS ---")
    billingRecordsDF.show(false)

    // ======================================
    // 11. UDF
    // ======================================

    val billingWithUDF = billingRecordsDF
      .withColumn(
        "udf_billing_status",
        classifyBillingStatus(col("outstanding_balance"))
      )

    println("\n--- UDF RESULT ---")
    billingWithUDF.show(false)

    // ======================================
    // 12. SPARK SQL
    // ======================================

    billingRecordsDF.createOrReplaceTempView("billing_records")

    val departmentRevenueSQL = spark.sql(
      """
        SELECT
          department,
          COUNT(*) AS visit_count,
          SUM(total_bill) AS total_revenue,
          AVG(total_bill) AS average_bill
        FROM billing_records
        GROUP BY department
        HAVING SUM(total_bill) > 1000
        ORDER BY total_revenue DESC
      """
    )

    println("\n--- SPARK SQL DEPARTMENT REVENUE ---")
    departmentRevenueSQL.show(false)

    // ======================================
    // 13. DATAFRAME AGGREGATION
    // ======================================

    val departmentRevenueDF = billingRecordsDF
      .groupBy("department")
      .agg(
        count("*").alias("visit_count"),
        sum("total_bill").alias("total_revenue"),
        avg("total_bill").alias("average_bill")
      )
      .filter(col("total_revenue") > 1000)
      .orderBy(desc("total_revenue"))

    println("\n--- DATAFRAME DEPARTMENT REVENUE ---")
    departmentRevenueDF.show(false)

    // ======================================
    // 14. PARTITION ANALYSIS
    // ======================================

    val originalPartitions =
      billingRecordsDF.rdd.getNumPartitions

    println("\n--- PARTITION ANALYSIS ---")
    println(s"Original partitions: $originalPartitions")

    val repartitionedDF =
      billingRecordsDF.repartition(4)

    println(
      s"After repartition(4): ${repartitionedDF.rdd.getNumPartitions}"
    )

    val coalescedDF =
      repartitionedDF.coalesce(2)

    println(
      s"After coalesce(2): ${coalescedDF.rdd.getNumPartitions}"
    )

    // ======================================
    // 15. PAIR RDD OPERATIONS
    // ======================================

    val departmentBillsRDD = billingRecordsDF.rdd
      .map(row => {

        val department =
          row.getAs[String]("department")

        val totalBill =
          row.getAs[Number]("total_bill").doubleValue()

        (department, totalBill)
      })

    // --------------------------------------
    // MAP
    // --------------------------------------

    println("\n--- MAP ---")

    val mappedRDD =
      departmentBillsRDD.map {
        case (department, bill) =>
          (department, bill * 1.0)
      }

    mappedRDD.collect().foreach(println)

    // --------------------------------------
    // FILTER
    // --------------------------------------

    println("\n--- FILTER (Bill > 1000) ---")

    val filteredRDD =
      mappedRDD.filter {
        case (_, bill) =>
          bill > 1000
      }

    filteredRDD.collect().foreach(println)

    // --------------------------------------
    // REDUCE BY KEY
    // --------------------------------------

    println("\n--- REDUCE BY KEY ---")

    val reducedRDD =
      departmentBillsRDD.reduceByKey(_ + _)

    reducedRDD.collect().foreach(println)

    // --------------------------------------
    // GROUP BY KEY
    // --------------------------------------

    println("\n--- GROUP BY KEY ---")

    val groupedRDD =
      departmentBillsRDD.groupByKey()

    groupedRDD.collect().foreach {
      case (department, bills) =>
        println(
          s"$department -> ${bills.toList}"
        )
    }

    // --------------------------------------
    // SORT BY KEY
    // --------------------------------------

    println("\n--- SORT BY KEY ---")

    val sortedRDD =
      departmentBillsRDD
        .reduceByKey(_ + _)
        .sortByKey()

    sortedRDD.collect().foreach(println)

    // ======================================
    // 16. DAG / STAGES / SHUFFLE
    // ======================================

    println("\n======================================")
    println("STEP 40 - DAG / STAGES / SHUFFLE")
    println("======================================")

    // Narrow transformation: MAP
    val dagMapRDD =
      departmentBillsRDD.map {
        case (department, bill) =>
          (department, bill * 1.0)
      }

    // Narrow transformation: FILTER
    val dagFilterRDD =
      dagMapRDD.filter {
        case (_, bill) =>
          bill > 1000
      }

    // Wide transformation: REDUCE BY KEY
    // reduceByKey causes a shuffle.
    val dagReduceRDD =
      dagFilterRDD.reduceByKey(_ + _)

    println("\n--- DAG RESULT ---")

    dagReduceRDD.collect().foreach(println)

    println("\n--- RDD DEBUG STRING ---")

    println(
      dagReduceRDD.toDebugString
    )

    println("\n--- TRANSFORMATION TYPES ---")

    println("MAP -> Narrow Transformation")
    println("FILTER -> Narrow Transformation")
    println("REDUCE BY KEY -> Wide Transformation")
    println("REDUCE BY KEY -> Causes Shuffle")
    println("Shuffle -> Creates Stage Boundary")

    // Force execution of the DAG
    dagReduceRDD.count()

    // ======================================
    // APPLICATION END
    // ======================================
        // ======================================
    // 17. ACCUMULATOR
    // ======================================

    println("\n======================================")
    println("STEP 41 - ACCUMULATOR")
    println("======================================")

    val outstandingAccumulator =
      spark.sparkContext.longAccumulator("Outstanding Bills")

    billingRecordsDF.rdd.foreach { row =>
      val balance =
        row.getAs[Number]("outstanding_balance").doubleValue()

      if (balance > 0.0) {
        outstandingAccumulator.add(1)
      }
    }

    println(
      s"Number of outstanding bills: ${outstandingAccumulator.value}"
    )
        // ======================================
    // 19. DATASET
    // ======================================

    println("\n======================================")
    println("STEP 43 - DATASET")
    println("======================================")

    import spark.implicits._

    val billingDataset =
      billingRecordsDF
        .select(
          "visit_id",
          "patient_id",
          "department",
          "total_bill",
          "insurance_covered",
          "amount_paid",
          "outstanding_balance"
        )
        .as[BillingRecord]

    println("\n--- DATASET RECORDS ---")

    billingDataset.show(false)

    println("\n--- DATASET TYPE-SAFE FILTER ---")

    val highValueBills =
      billingDataset.filter(_.total_bill > 2000)

    highValueBills.show(false)

    println(
      s"High-value bills count: ${highValueBills.count()}"
    )

      // ======================================
    // 20. PARTITIONED OUTPUT
    // ======================================

    println("\n======================================")
    println("STEP 44 - PARTITIONED OUTPUT")
    println("======================================")

    val outputPath = "data/output/billing_records"

    billingRecordsDF
      .repartition(col("department"))
      .write
      .mode("overwrite")
      .partitionBy("department")
      .option("header", "true")
      .csv(outputPath)

      println(
      s"Billing records written successfully to: $outputPath"
    )

    println("Output is partitioned by department.")
    println("\n======================================")
    println("Hospital Billing Analytics Completed!")
    println("======================================")

    spark.stop()
  }
}

