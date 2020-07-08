package playground

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Dataframes extends App {
  /**
    * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    */
  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
    */
  val sc = spark.sparkContext

  // create DF from tuples
  val smartphonesTuple = Seq(
    ("Samsung", "12 Edge +", 6.2, 24.0),
    ("OnePlus", "X", 6.2, 36.0),
    ("Apple", "12R", 3.5, 8.0),
    ("Apple", "12S", 4.7, 12.0),
  )
  val manualSmartphonesDF = spark.createDataFrame(smartphonesTuple)
  manualSmartphonesDF.printSchema()
  manualSmartphonesDF.show()

  import spark.implicits._
  val implicitsSmartphoneDF = smartphonesTuple.toDF("Make","Model","Screen Size","Resolution")
  implicitsSmartphoneDF.printSchema()
  implicitsSmartphoneDF.show()

  val smartphones = Seq(
    Row("Samsung", "12 Edge +", 6.2, 24.0),
    Row("OnePlus", "X", 6.2, 36.0),
    Row("Apple", "12R", 3.5, 8.0),
    Row("Apple", "12S", 4.7, 12.0),
  )
  val smartphoneSchema = StructType(Array(
    StructField("Make", StringType),
    StructField("Model", StringType),
    StructField("Screen Size", DoubleType),
    StructField("Camera Resolution", DoubleType))
  )
  val smartphoneRows = sc.parallelize(smartphones)
  val manualSmartphonesRowsDF = spark.createDataFrame(smartphoneRows, smartphoneSchema)
  manualSmartphonesRowsDF.printSchema()
  manualSmartphonesRowsDF.show()

  val moviesSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", LongType),
    StructField("Worldwide_Gross", LongType),
    StructField("US_DVD_Sales", LongType),
    StructField("Production_Budget", LongType),
    StructField("Release_Date", DateType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min", IntegerType),
    StructField("Distributor", StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", IntegerType),
    StructField("IMDB_Rating", FloatType),
    StructField("IMDB_Votes", LongType)
  )
  )

  val moviesDF = spark.read
    .format("json")
    .schema(moviesSchema)
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  moviesDF.show()
  println($"The movies dataframe has ${moviesDF.count()} movies in total")

}
