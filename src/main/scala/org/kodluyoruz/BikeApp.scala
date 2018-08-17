package org.kodluyoruz

import org.apache.spark.sql.SparkSession
import org.kodluyoruz.model.{RawBikeStationData, RawBikeTripData, Result}

object BikeApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kodluyoruz-spark-project")
      .master("local[2]")
      .getOrCreate()


    val stationPath = "/Users/ycan/Kodluyoruz/Spark-The-Definitive-Guide/data/bike-data/201508_station_data.csv"
    val tripPath = "/Users/ycan/Kodluyoruz/Spark-The-Definitive-Guide/data/bike-data/201508_trip_data.csv"
    import spark.implicits._

    val stationData = spark.read.option("header", true).csv(stationPath)//.as[RawBikeStationData]
    val tripData = spark.read.option("header", true).csv(tripPath)//.as[RawBikeTripData]

    val t = tripData.join(stationData, tripData("Start Station") === stationData("name"))
    .as[Result]


    t.take(10).foreach(println)

  }

}
