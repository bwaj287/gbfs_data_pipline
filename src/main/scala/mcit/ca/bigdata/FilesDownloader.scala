package mcit.ca.bigdata

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object FilesDownloader extends App {

  val si = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/station_information.json")
  val s = si.mkString

  def siwriteFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
  try{
  siwriteFile("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/station_information/station_information.json", s)
  }catch{
        case e: Exception => println(e)
      }

  val sys = Source.fromURL("https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_pp/en/system_information.json")
  val sy = sys.mkString

  def sywriteFile(filename: String, sy: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sy)
    bw.close()
  }
try {
  sywriteFile("hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/system_information/system_information.json", sy)
}catch{
     case e: Exception => println(e)
   }

  println("Source files auto downloaded into LFS")
}
