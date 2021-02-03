package mcit.ca.bigdata

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object FilesDownloader {
  val stationInfoUrl = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/station_information.json")
  val strStation = stationInfoUrl.mkString

  def stationInfoWriteFile(filename: String, strStation: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(strStation)
    bw.close()
  }

  stationInfoWriteFile("/home/vasu/Downloads/Gbfs_downloads/station_information.json", strStation)

  val sysstemInfoUrl = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/system_information.json")
  val strSystem = sysstemInfoUrl.mkString

  def systemInfoWriteFile(filename: String, strSystem: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(strSystem)
    bw.close()
  }

  stationInfoWriteFile("/home/vasu/Downloads/Gbfs_downloads/system_information.json", strSystem)

}
