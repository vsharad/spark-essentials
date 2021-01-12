package datatypesAndDatasets

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Game-dataSet-Match")
    .config("spark.master","local")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")
  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  val carsDS = carsDF.as[Car]
  val carsCount = carsDS.count()
  println(s"Cars Count = $carsCount")

  val powerful = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count

  println(s"Powerful Cars = $powerful")

  val averageHorsepower = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount
  println(s"Average Horsepower = $averageHorsepower")

  carsDS.select(avg(col("Horsepower"))).show()

  val carsGroupedByOrigin: Unit = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  case class Guitar(id: Long, make: String, model: String, `type`: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  def readDF(fileName: String):DataFrame = spark.read
    .option("inferSchema","true")
    .json(s"src/main/resources/data/$fileName")

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    */
  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

}
