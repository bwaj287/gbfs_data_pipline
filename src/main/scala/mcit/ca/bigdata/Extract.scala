package mcit.ca.bigdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait  Main {

  val conf = new Configuration()

  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/vasusurani/opt/hadoop-2.7.7/etc/cloudera/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)

}
