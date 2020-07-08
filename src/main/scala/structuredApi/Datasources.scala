package structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Datasources extends App {
  val spark = SparkSession.builder()
    .appName("Datasources in Spark")
    .config("spark.master","local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map(
      "mode" -> "failFast"
      ,"path" -> "src/main/resources/data/cars.json"
      ,"dateFormat" -> "YYYY-MM-dd"
      ,"allowSingleQuotes" -> "true"
      ,"compression" -> "uncompressed"
    ))
    .load()

  carsDF.show()
  spark.close()
}
