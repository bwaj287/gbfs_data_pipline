package mcit.ca.bigdata

import java.sql.{Connection, DriverManager, Statement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object Conversion extends App{

  val conf = new Configuration()
  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/winter2020_vasu;user=vasu;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

  val systemInformationSchema: StructType = Encoders.product[SystemInformation].schema
  val stationInformationSchema: StructType = Encoders.product[StationInformation].schema

  val stationInformation: String = "/Users/vasusurani/Downloads/Final Project Big Data/data2/station_information.json"
  val systemInformation: String = "/Users/vasusurani/Downloads/Final Project Big Data/data2/system_information.json"

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Conversion").getOrCreate()

  val stationInformationData: DataFrame = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(stationInformation)
  val systemInformationData: DataFrame = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(systemInformation)

  val stationInfoDataRetriving: DataFrame = stationInformationData.select(col("data").getField("stations").as("stations"))

  val stationData: DataFrame =
    stationInfoDataRetriving.withColumn("stations", explode(col("stations"))).select(
      col("stations").getField("station_id").as("station_id"),
      col("stations").getField("name").as("station_name"),
      col("stations").getField("short_name").as("short_name"),
      col("stations").getField("lat").as("lat"),
      col("stations").getField("lon").as("lon"),
      col("stations").getField("region_id").as("region_id"),
      col("stations").getField("capacity").as("capacity"))

  val systemData: DataFrame =
    systemInformationData.select(
      col("last_updated").as("last_updated"),
      col("ttl").as("ttl"),
      col("data").getField("system_id").as("system_id"),
      col("data").getField("language").as("language"),
      col("data").getField("name").as("name"),
      col("data").getField("operator").as("operator"),
      col("data").getField("url").as("url"),
      col("data").getField("phone_number").as("phone_number"),
      col("data").getField("email").as("email"),
      col("data").getField("timezone").as("timezone"))

  val systemDataCsvFile: DataFrame = systemData
  systemDataCsvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("/Users/vasusurani/Downloads/Final Project Big Data/systemData")
  println("system data csv file saved successfully")

  //val filePath = new Path("")hdfs://quickstart.cloudera:8020/user/winter2020/vasu/enriched_station_information

  val stationDataCsvFile: DataFrame = stationData
  stationDataCsvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("/Users/vasusurani/Downloads/Final Project Big Data/stationData")
  println("stationdata csv file saved successfully")

  val csvFile: DataFrame = stationData.crossJoin(systemData)
  csvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("/Users/vasusurani/Downloads/Final Project Big Data/enrichedStationData")

  stmt.execute("DROP TABLE IF EXISTS winter2020_vasu.enriched_station_information")
  val enrichedStationInfo = new EnrichedTable().enrichedStationInfo
  stmt.execute(enrichedStationInfo)

/*
  val wrData = new WriteData().writeTrips
  stmt.execute(wrData)
*/
}
