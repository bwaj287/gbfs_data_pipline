package mcit.ca.bigdata

class StationInformationTable {
  val stationInformationTable =
    """CREATE TABLE winter2020_vasu.ext_station_information (
      |name                           STRING,
      |station_id                     STRING,
      |short_name                     STRING,
      |lat                            STRING,
      |lon                            STRING,
      |region_id                      INT,
      |capacity                       INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/vasu/final_project/ext_station_information'
      |TBLPROPERTIES (
      |"skip.header.line.count" = "1",
      |"serialization.null.format" = "") """.stripMargin

  println("ext_station_information TABLE was CREATED")

}
