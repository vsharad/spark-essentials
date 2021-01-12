package datatypesAndDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date,col}

object ComplexDatatypes extends App{
  val spark = SparkSession.builder()
    .appName("Datatypes can be complex as well")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext

  val stocksDF = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("src/main/resources/data/stocks.csv")

  val stocksDateFormattedDF = stocksDF.withColumn("actual_date",to_date(col("date"),"MMM d yyyy"))
  stocksDateFormattedDF.show()

}
