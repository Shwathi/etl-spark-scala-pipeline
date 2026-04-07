package com.project

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    // Fix for Windows Hadoop issue
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("spark.hadoop.io.native.lib.available", "false")

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("ETL Pipeline - Scala Spark")
      .master("local[*]")
      .config("spark.hadoop.io.native.lib.available", "false")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp")
      .getOrCreate()

    // Step 1: Extract
    val df = Extract.readData(spark, "data/sales.csv")

    println("=== Raw Data ===")
    df.show(false)

    // Step 2: Clean & Transform
    val cleanedDF = Transform.cleanData(df)

    println("=== Cleaned Data ===")
    cleanedDF.show(false)

    // Step 3: Product Analysis
    val productDF = Transform.productAnalysis(cleanedDF)

    println("=== Product Analysis ===")
    productDF.show(false)

    // Step 4: City Analysis
    val cityDF = Transform.cityAnalysis(cleanedDF)

    println("=== City Analysis ===")
    cityDF.show(false)

    // Step 5: Payment Analysis
    val paymentDF = Transform.paymentAnalysis(cleanedDF)

    println("=== Payment Analysis ===")
    paymentDF.show(false)

    // Step 6: Load
    Load.writeData(productDF, "file:///C:/spark-output/")

    // Stop Spark
    spark.stop()
  }
}
