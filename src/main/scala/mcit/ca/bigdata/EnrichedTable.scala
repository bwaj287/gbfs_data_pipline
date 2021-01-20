package mcit.ca.bigdata

class EnrichedTable {
  val delTable = """DROP TABLE IF EXISTS winter2020_vasu.enriched_station_information"""
  val enrichedStationInfo =
    """CREATE TABLE winter2020_vasu.enriched_station_information (
      |station_id                     STRING,
      |short_name                     STRING,
      |lat                            STRING,
      |lon                            STRING,
      |region_id                      INT,
      |capacity                       INT,
      |last_updated                   INT,
      |ttl                            INT,
      |system_id                      STRING,
      |language                       STRING,
      |name                           STRING,
      |operator                       STRING,
      |url                            STRING,
      |phone_number                   STRING,
      |email                          STRING,
      |timezone                       STRING
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/vasu/final_project/enriched_station_information'
      |TBLPROPERTIES (
      | "skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  println("enriched_station_system TABLE was CREATED")

}
