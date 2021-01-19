name := "gbfs_data_pipline"

version := "0.1"

scalaVersion := "2.12.8"


val sparkVersion = "2.4.4"

val hadoopVersion = "2.7.7"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"