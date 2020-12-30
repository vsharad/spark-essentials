package datatypesAndDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract,col}

object CommonDatatypes extends App{
  val spark = SparkSession.builder()
    .appName("Common datatypes are not so common anymore!")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  def getCarNames:List[String] = List("Volkswagen","Mercedes-Benz","Ford")
  val carNamesRegex = getCarNames.map(_.toLowerCase()).mkString("|")

  val selectedCarsDF = carsDF.select(col("name"),
    regexp_extract(col("name"),carNamesRegex,0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  selectedCarsDF.show()

}