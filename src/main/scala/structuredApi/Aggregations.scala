package structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  moviesDF.selectExpr("sum(Worldwide_Gross+US_Gross+US_DVD_Sales)").show()

  moviesDF.select(expr("US_Gross + Worldwide_Gross + US_DVD_Sales").as("Total_Gross"))
  .select(sum("Total_Gross")).show()

  moviesDF.select(countDistinct(col("Director"))).show()

  moviesDF.select(
    mean("US_Gross")
    ,stddev("US_Gross")
  ).show()

  moviesDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_Rating")
      ,avg("US_Gross").as("Avg_Gross")
    ).orderBy(desc("Avg_Rating")).show()


}
