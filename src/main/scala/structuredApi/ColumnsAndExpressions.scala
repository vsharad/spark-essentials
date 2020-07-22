package structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App{
  val spark = SparkSession.builder()
    .appName("Spark Columns and Expressions App")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  import spark.implicits._
  val moviesColumns1 = moviesDF.select(
    moviesDF.col("Title")
    ,col("Release_Date")
    ,column("Distributor")
    ,'Source
    ,$"Director"
    ,expr("Creative_Type")
  )

  val moviesColumns2 = moviesDF.select("Title","Release_Date")

  val moviesColumns3 = moviesDF.selectExpr(
    "Title"
    ,"Worldwide_Gross"
    ,"US_Gross"
    ,"US_DVD_Sales"
    ,"US_Gross + Worldwide_Gross as Total_Gross"
  )
  val moviesColumns4 = moviesDF.select(
    $"Title"
    ,$"Worldwide_Gross"
    ,$"US_Gross"
    ,$"US_DVD_Sales"
    ,expr("US_Gross + Worldwide_Gross").as("Total_Gross")
  )

  val moviesColumns5 = moviesDF.select("Title","Release_Date", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross",$"US_Gross"+$"Worldwide_Gross").show()

  val comedyMovies1 = moviesDF.select("Title","IMDB_Rating")
    .where($"Major_Genre" === "Comedy")
    .where($"IMDB_Rating" > 6)

  val comedyMovies2 = moviesDF.select("Title","IMDB_Rating")
    .where($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6)

  val comedyMovies3 = moviesDF.select("Title","IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
}