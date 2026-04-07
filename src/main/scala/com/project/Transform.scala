package com.project

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, desc, sum}

object Transform {

  def cleanData(df: DataFrame): DataFrame = {

    val cleanedDF = df
      .na.fill(Map(
      "quantity" -> 1,
      "price" -> 0,
      "city" -> "Unknown"
    ))
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("total_amount", col("quantity") * col("price"))

    cleanedDF
  }

  // 🔹 Product-wise analysis
  def productAnalysis(df: DataFrame): DataFrame = {
    df.groupBy("product", "category")
      .agg(
        sum("total_amount").alias("total_sales"),
        count("order_id").alias("total_orders")
      )
      .orderBy(desc("total_sales"))
  }

  // 🔹 City-wise analysis
  def cityAnalysis(df: DataFrame): DataFrame = {
    df.groupBy("city")
      .agg(
        sum("total_amount").alias("city_total_sales"),
        count("order_id").alias("city_total_orders")
      )
      .orderBy(desc("city_total_sales"))
  }

  // 🔹 Payment method analysis
  def paymentAnalysis(df: DataFrame): DataFrame = {
    df.groupBy("payment_method")
      .agg(
        sum("total_amount").alias("payment_total_sales"),
        count("order_id").alias("payment_total_orders")
      )
      .orderBy(desc("payment_total_sales"))
  }
}
