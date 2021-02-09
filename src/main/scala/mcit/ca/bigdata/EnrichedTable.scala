package mcit.ca.bigdata

class EnrichedTable {
  val enrichStatInfoInsertion =
    """
      |INSERT OVERWRITE DIRECTORY '/user/winter2020/vasu/finalproject/Enriched'
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |SELECT * from winter2020_vasu.gjo_system_information join winter2020_vasu.station_information
      |""".stripMargin

  val extEnrichedStatInfo =
    """
      |CREATE external TABLE winter2020_vasu.enriched_station_information(
      |system_Id STRING,
      |timezone STRING,
      |station_Id INT,
      |name STRING,
      |short_name INT,
      |capacity INT,
      |latitude FLOAT,
      |longitude FLOAT,
      |region_id INT)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION 'hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/Enriched'
      |""".stripMargin

}
