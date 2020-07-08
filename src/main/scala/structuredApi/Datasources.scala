package structuredApi

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Datasources extends App {
  val spark = SparkSession.builder()
    .appName("Datasources in Spark")
    .config("spark.master","local")
    .getOrCreate()


  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("sep","\t")
      .option("header","true")
      .save("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  val dbtable = "public.movies"

  moviesDF.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .options(Map(
        "driver" -> driver
        ,"url" -> url
        ,"user" -> user
        ,"password" -> password
        ,"dbtable" -> dbtable
      ))
      .save()
  moviesDF.printSchema()
  moviesDF.show()
}
