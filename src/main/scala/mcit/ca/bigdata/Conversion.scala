package mcit.ca.bigdata

import java.sql.{Connection, DriverManager}

object Conversion extends FilesDownloader with App {

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection(
    "jdbc:hive2://quickstart.cloudera:10000/winter2020_vasu;user=vasu;")
  val stmt = connection.createStatement()

  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute(" SET hive.auto.convert.join=true ")

  // => Station_information.json part

  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.string_station_information ")
  val stringStationInfoTable = new StationInformationTable().stringStationInformationTable
  stmt.execute(stringStationInfoTable)
  println("String Station Information table created")

  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.json_station_information ")
  val jsonStationInfoTable = new StationInformationTable().jsonStationInformationTable
  stmt.execute(jsonStationInfoTable)
  println("Json Station Information table created")

  val jsonStationInfoInsertion = new StationInformationTable().jsonStationInformationInsertion
  stmt.execute(jsonStationInfoInsertion)
  println("Insertion in Json Station Information table is Complete")

  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.gjo_station_information ")
  val gjoStatInfoTable = new StationInformationTable().gjoStaionInformationTable
  stmt.execute(gjoStatInfoTable)
  println("Get Json Object Station Information table Created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.sid_name")
  val tableSidName = new StationInformationTable().tableStationIdName
  stmt.execute(tableSidName)
  println("Sid Name table created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.slat_lon")
  val tableStatLatLon = new StationInformationTable().tableLatLon
  stmt.execute(tableStatLatLon)
  println("SLat and Lon table created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.sname_cap")
  val tableShortNameCap = new StationInformationTable().tableSnameCapacity
  stmt.execute(tableShortNameCap)
  println("SName table created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.rid")
  val tableRegionId = new StationInformationTable().tableRegId
  stmt.execute(tableRegionId)
  println("Rid table created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.station_information")
  val extStationInformation = new StationInformationTable().extStatInfo
  stmt.execute(extStationInformation)
  println("Station Information table created")

  // => system_information.json part

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.string_system_information")
  val stringSystemInformation = new SystemInformationTable().stringSysInfoTable
  stmt.execute(stringSystemInformation)
  println("String System Information table created")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.json_system_information")
  val jsonSystemInformationTable = new SystemInformationTable().jsonSysInfoTable
  stmt.execute(jsonSystemInformationTable)
  println("Json System Information table created")

  val jsonSystemInformationInsertion = new SystemInformationTable().jsonSysInfoInsertion
  stmt.execute(jsonSystemInformationInsertion)
  println("Insertion in Json System Information table is Complete")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.gjo_system_information")
  val extSystemInformation = new SystemInformationTable().extSysInfo
  stmt.execute(extSystemInformation)
  println("Get Json Object System Information table created")

  // => Enrichment part

  val enrichStationInformationInsertion = new EnrichedTable().enrichStatInfoInsertion
  stmt.execute(enrichStationInformationInsertion)
  println("Joining of System Information and Station Information is Complete")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.enriched_station_information")
  val extEnrichedStationInformation = new EnrichedTable().extEnrichedStatInfo
  stmt.execute(extEnrichedStationInformation)
  println("Enriched Station Information table created")

  stmt.close()
  connection.close()
}