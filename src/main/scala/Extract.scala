import org.apache.spark.sql.{DataFrame, SparkSession}

object Extract {
  def readData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(path)
  }
}