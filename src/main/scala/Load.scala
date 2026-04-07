import org.apache.spark.sql.DataFrame

object Load {
  def writeData(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }
}