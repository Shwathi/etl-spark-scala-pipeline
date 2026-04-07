import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ETL Pipeline")
      .master("local[*]")
      .config("spark.hadoop.io.native.lib.available", "false")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp")
      .getOrCreate()

    val df = Extract.readData(spark, "data/sales.csv")
    val transformedDF = Transform.cleanData(df)
    Load.writeData(transformedDF, "output/")
    transformedDF.show(false)

    spark.stop()
  }
}