package mcit.ca.bigdata

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object Conversion extends App {

  val systemInformationSchema: StructType = Encoders.product[SystemInformation].schema
  val stationInformationSchema: StructType = Encoders.product[StationInformation].schema

  val stationInformation: String = "/Users/vasusurani/Downloads/Final Project Big Data/Data/station_information.json"
  val systemInformation: String = "/Users/vasusurani/Downloads/Final Project Big Data/Data/system_information.json"

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
      col("stations").getField("rental_methods").getItem(0).as("rental_methods"),
      col("stations").getField("rental_methods").getItem(1).as("rental_methods_1"),
      col("stations").getField("capacity").as("capacity"),
      col("stations").getField("electric_bike_surcharge_waiver").as("electric_bike_surcharge_waiver"),
      col("stations").getField("is_charging").as("is_charging"),
      col("stations").getField("eightd_has_key_dispenser").as("eightd_has_key_dispenser"),
      col("stations").getField("has_kiosk").as("has_kiosk"))

  val systemData: DataFrame =
    systemInformationData.select(
      col("last_updated").as("last_updated"),
      col("ttl").as("ttl"),
      col("data").getField("system_id").as("system_id"),
      col("data").getField("language").as("language"),
      col("data").getField("name").as("name"),
      col("data").getField("operator").as("operator"),
      col("data").getField("url").as("url"),
      col("data").getField("purchase_url").as("purchase_url"),
      col("data").getField("phone_number").as("phone_number"),
      col("data").getField("email").as("email"),
      col("data").getField("license_url").as("license_url"),
      col("data").getField("timezone").as("timezone"))

  val systemDataCsvFile: DataFrame = systemData
  systemDataCsvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("/Users/vasusurani/Downloads/Final Project Big Data/systemData")
  println("system data csv file saved successfully")

  val stationDataCsvFile: DataFrame = stationData
  stationDataCsvFile.write.format("com.databricks.spark.csv").option("header", "True").mode("overwrite").option("sep", ",")
    .save("/Users/vasusurani/Downloads/Final Project Big Data/stationData")
  println("stationdata csv file saved successfully")
}

