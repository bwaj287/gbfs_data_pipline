package mcit.ca.bigdata
import java.sql.{Connection, DriverManager}
import java.util.Calendar

object Conversion extends Main with App {

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/winter2020_vasu;user=vasu;")
  val stmt = connection.createStatement()
  println("JSON Source files auto downloaded and uploaded to HDFS from LFS")
  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
  println("1")
  stmt.execute(" SET hive.auto.convert.join=true")
  println("2")
  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.string_station_information ")
  println("3")
  val stringStationInfoTable = new StationInformationTable().stringStationInformationTable
  stmt.execute(stringStationInfoTable)
  println("4")
  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.fj_station_information ")
  println("5")
  val jsonStationInfoTable = new StationInformationTable().jsonStationInformationTable
  stmt.execute(jsonStationInfoTable)
  println("6")
  val jsonStationInfoInsertion = new StationInformationTable().jsonStationInformationInsertion
  stmt.execute(jsonStationInfoInsertion)
  println("7")
  stmt.execute(" DROP TABLE IF EXISTS winter2020_vasu.gjo_station_information")
  println("8")
//  val getJsonObjStationInfoTable = new StationInformationTable().getJsonObjectStationInformationTable
//  stmt.execute(getJsonObjStationInfoTable)
  stmt.execute(" CREATE TABLE winter2020_vasu.gjo_station_information AS SELECT " +
    "split(get_json_object(c.json_body,'$.data.stations.station_id'),\",\") as id,  " +
    "split(get_json_object(c.json_body,'$.data.stations.name'),\",\") as name,  " +
    "split(get_json_object(c.json_body,'$.data.stations.short_name'),\",\") as short_name, " +
    "split(get_json_object(c.json_body,'$.data.stations.lat'),\",\") as lat, " +
    "split(get_json_object(c.json_body,'$.data.stations.lon'),\",\") as lon, " +
    "split(get_json_object(c.json_body,'$.data.stations.region_id'),\",\") as region_id, " +
    "split(get_json_object(c.json_body,'$.data.stations.capacity'),\",\") as capacity " +
    "FROM winter2020_vasu.fj_station_information c")
  println("9")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.sid_name")
  println("10")
  val tableSidName = new StationInformationTable().tableStationIdName
  stmt.execute(tableSidName)
  println("11")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.slat_lon")
  println("12")
  val tableStatLatLon = new StationInformationTable().tableLatLon
  stmt.execute(tableStatLatLon)
  println("13")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.sname_cap")
  println("14")
  val tableShortNameCap = new StationInformationTable().tableSnameCapacity
  stmt.execute(tableShortNameCap)
  println("15")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.rid")
  println("16")
  val tableRegionId = new StationInformationTable().tableRegId
  stmt.execute(tableRegionId)
  println("17")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.station_information")
  println("18")
  val extStationInformation = new StationInformationTable().extStatInfo
  stmt.execute(extStationInformation)
  println("19")
  println(Calendar.getInstance().getTime)
  println("20")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.string_system_information")
  println("21")
  val stringSystemInformation = new SystemInformationTable().stringSysInfoTable
  stmt.execute(stringSystemInformation)
  println("22")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.fj_system_information")
  println("23")
  val jsonSystemInformationTable = new SystemInformationTable().jsonSysInfoTable
  stmt.execute(jsonSystemInformationTable)
  println("24")
  val jsonSystemInformationInsertion = new SystemInformationTable().jsonSysInfoInsertion
  stmt.execute(jsonSystemInformationInsertion)
  println("25")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.system_information")
  println("26")
  val extSystemInformation = new SystemInformationTable().extSysInfo
  stmt.execute(extSystemInformation)
  println("27")
  println(Calendar.getInstance().getTime)
  println("28")
  val enrichStationInformationInsertion = new EnrichedTable().enrichStatInfoInsertion
  stmt.execute(enrichStationInformationInsertion)
  println("29")
  println(Calendar.getInstance().getTime)
  println("30")
  println("Enriched file in HDFS")
  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.enriched_station_information")
  println("31")
  val extEnrichedStationInformation = new EnrichedTable().extEnrichedStatInfo
  stmt.execute(extEnrichedStationInformation)
  println("External table for enriched station_information created in HIVE")

  stmt.close()
  connection.close()
}