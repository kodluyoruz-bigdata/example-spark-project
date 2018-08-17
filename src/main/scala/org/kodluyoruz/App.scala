package org.kodluyoruz

import org.apache.spark.sql.SparkSession
import org.kodluyoruz.model.RawFlightData

object App {

  def main(args: Array[String]): Unit = {

    val filePath = args(0)

    val sparkSession = SparkSession
      .builder()
      .appName("kodluyoruz-spark-project")
//      .master("local[2]")
      .getOrCreate()

    val rawData = sparkSession.read.json(filePath)

    import sparkSession.implicits._
    val castedFlightData = rawData.as[RawFlightData]

    println(s"Number of flight data: ${castedFlightData.count()}")

  }

}
