package lowLevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("RDDs can be very resilient")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  import spark.implicits._
  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  moviesRDD.take(5).foreach(println)

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()
  genresRDD.foreach(println)

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  goodDramasRDD.take(5).foreach(println)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }
  avgRatingByGenreRDD.foreach(println)

  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}