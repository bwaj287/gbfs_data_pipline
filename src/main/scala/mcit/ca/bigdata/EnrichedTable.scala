package mcit.ca.bigdata

class EnrichedTable {
  val enrichStatInfoInsertion =
    """
      |INSERT OVERWRITE DIRECTORY '/user/winter2020/vasu/finalproject/Enriched'
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |SELECT * from winter2020_vasu.system_information join winter2020_vasu.station_information
      |""".stripMargin

  val extEnrichedStatInfo =
    """
      |CREATE external TABLE winter2020_vasu.enriched_station_information(
      |system_Id STRING,
      |language STRING,
      |url STRING,
      |timezone STRING,
      |station_Id INT,
      |name STRING,
      |short_name INT,
      |latitude FLOAT,
      |longitude FLOAT,
      |capacity INT,
      |region_id STRING)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION 'hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/Enriched'
      |""".stripMargin

  println("enriched_station_information TABLE was CREATED")

}
