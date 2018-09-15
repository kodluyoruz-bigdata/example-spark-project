package org.kodluyoruz.model

/*
{"ORIGIN_COUNTRY_NAME":"Romania",
"DEST_COUNTRY_NAME":"United States",
"count":1}
 */

case class RawFlightData(ORIGIN_COUNTRY_NAME: String,
                         DEST_COUNTRY_NAME: String,
                         count: Long)

case class FlightAggResult(key: String, value: Long)

case class RawBikeStationData(station_id: String,
                              name: String,
                              lat: String,
                              `long`: String,
                              dockcount: String,
                              landmark: String,
                              installation: String)

case class RawBikeTripData(`Trip ID`: String,
                            Duration: String,
                           `Start Date`: String,
                           `Start Station`: String,
                           `Start Terminal`: String,
                           `End Date`: String,
                           `End Station`: String,
                           `End Terminal`: String,
                           `Bike #`:String,
                           `Subscriber Type`: String,
                           `Zip Code`: String)


case class Result(station_id: String,
                  name: String,
                  lat: String,
                  long: String,
                  dockcount: String,
                  landmark: String,
                  installation: String,
                  `Trip ID`: String,
                  Duration: String,
                  `Start Date`: String,
                  `Start Station`: String,
                  `Start Terminal`: String,
                  `End Date`: String,
                  `End Station`: String,
                  `End Terminal`: String,
                  `Bike #`:String,
                  `Subscriber Type`: String,
                  `Zip Code`: String
                 ){
  override def toString: String = s"""Trip Id: ${`Trip ID`}, Station Name: $name, Latitude: $lat, Longitude: ${`long`}"""

}