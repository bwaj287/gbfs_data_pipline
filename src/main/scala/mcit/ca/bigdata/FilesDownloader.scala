package mcit.ca.bigdata

import java.io.{File,BufferedWriter,FileWriter}
import scala.io.Source

class FilesDownloader extends Main {

  try{
  val statInfoUrl = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/station_information.json")
  val st = statInfoUrl.mkString
  def siWriteFile(filename: String, st: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(st)
    bw.close()
  }
  siWriteFile("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/" +
    "station_information/station_information.json", st)
  }catch {
    case e:Exception => println(e)
  }

  try{
  val sysInfoUrl = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/system_information.json")
  val sy = sysInfoUrl.mkString
  def syWriteFile(filename: String, sy: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sy)
    bw.close()
  }
  syWriteFile("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/" +
    "system_information/system_information.json", sy)
  }catch {
    case e:Exception => println(e)
  }
}


