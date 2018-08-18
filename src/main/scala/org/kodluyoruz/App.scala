package org.kodluyoruz

import org.apache.spark.sql.SparkSession
import org.kodluyoruz.model.RawFlightData

object App {

  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val outputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .appName("kodluyoruz-spark-project")
      .getOrCreate()

    val rawData = sparkSession.read.json(filePath)

    import sparkSession.implicits._
    val castedFlightData = rawData.as[RawFlightData]

    val tt = castedFlightData.groupByKey(r => r.ORIGIN_COUNTRY_NAME).count()

    tt.write.json(outputPath)

  }

}
