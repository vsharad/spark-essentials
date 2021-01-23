package taxiData

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App {
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()
  import spark.implicits._

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  // taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  // taxiZonesDF.printSchema()

  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed by length?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    *
    */

  // 1

  val pickupAnalysis =  taxiDF.groupBy("PULocationID")
    .agg(count("*").as("Total Trips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("PULocationID", "service_zone")
    .orderBy(col("Total Trips").desc_nulls_last)

  // pickupAnalysis.show()

  val dropoffAnalysis =  taxiDF.groupBy("DOLocationID")
    .agg(count("*").as("Total Trips"))
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .drop("DOLocationID", "service_zone")
    .orderBy(col("Total Trips").desc_nulls_last)

  // dropoffAnalysis.show()

  val pickup = taxiDF.as("t")
    .join(taxiZonesDF.as("tZPU"), col("PULocationID") === col("tZPU.LocationID"))
    .select(col("LocationID"))

  val dropOff = taxiDF.as("t")
    .join(taxiZonesDF.as("tZDO"), col("DOLocationID") === col("tZDO.LocationID"))
    .select(col("LocationID"))

  val combinedTrips = pickup.union(dropOff)
  val pickupDropoffAnalysis = combinedTrips
    .groupBy(col("LocationID"))
    .agg(count("*").as("Total Trips"))
    .orderBy(col("Total Trips").desc_nulls_last)

  // pickupDropoffAnalysis.show()

  // 2
  val peakHour = taxiDF
    .withColumn("HourOfTheDay", hour(col("tpep_pickup_datetime")))
    .groupBy(col("HourOfTheDay"))
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

   // peakHour.show()

  // 3
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = taxiDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("trip_distance").as("mean"),
    stddev("trip_distance").as("stddev"),
    min("trip_distance").as("min"),
    max("trip_distance").as("max")
  )
  // tripDistanceStatsDF.show()

  val tripsByLengthDF = taxiDF
    .withColumn("IsLong", col("trip_distance") >= longDistanceThreshold)
    .groupBy("IsLong")
    .count()
  // tripsByLengthDF.show()

  // 4
  val tripsByLengthPeakHourDF = taxiDF
    .withColumn("IsLong", col("trip_distance") >= longDistanceThreshold)
    .withColumn("HourOfTheDay", hour(col("tpep_pickup_datetime")))
    .groupBy("IsLong","HourOfTheDay")
    .count()

  // tripsByLengthPeakHourDF.show()

  // 5

  val pickupDropoffPopularity = taxiDF
    .withColumn("IsLong", col("trip_distance") >= longDistanceThreshold)
    .where(not(col("IsLong")))
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)


  // pickupDropoffPopularity.show(3)


  // 6
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID")).agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

  // ratecodeDistributionDF.show()

  // 7
  val ratecodeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("pickup_day"))

  // ratecodeEvolution.show()

  // 8
  val groupAttemptsDF = taxiDF
    .select(
      round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId")
      , col("PULocationID")
      , col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  //groupAttemptsDF.show()

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))

  totalProfitDF.show()

}
