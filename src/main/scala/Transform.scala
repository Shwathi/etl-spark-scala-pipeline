import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transform {
  def cleanData(df: DataFrame): DataFrame = {

    df.na.drop() // remove null rows
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("total_amount", col("quantity") * col("price"))
      .groupBy("product", "category")
      .agg(
        sum("total_amount").alias("total_sales"),
        count("order_id").alias("total_orders")
      )
      .orderBy(desc("total_sales"))
  }
}