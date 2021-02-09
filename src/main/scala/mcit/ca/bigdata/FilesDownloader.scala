package mcit.ca.bigdata

import java.io.{File, PrintWriter}
import scala.io.{BufferedSource, Source}

class FilesDownloader {

  try {
  val systemInformation: BufferedSource = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/system_information.json")
  val systemInfo: String = systemInformation.mkString
  val systemInfoWriter = new PrintWriter(new File("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/system_information/system_information.json"))
  systemInfoWriter.write(systemInfo)
  systemInfoWriter.close()
  println("System Information File Downloaded!")
  }catch{
    case e: Exception => println(e)
  }

  try{
  val stationInformation: BufferedSource = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/station_information.json")
  val stationInfo: String = stationInformation.mkString
  val stationInfoWriter = new PrintWriter(new File("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/station_information/station_information.json"))
  stationInfoWriter.write(stationInfo)
  stationInfoWriter.close()
  println("Station Information File Downloaded!")
  }catch{
    case e: Exception => println(e)
  }
}

