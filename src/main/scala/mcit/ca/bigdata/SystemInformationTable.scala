package mcit.ca.bigdata

class SystemInformationTable {
  val systemInformationTable =
    """CREATE TABLE winter2020_vasu.ext_system_information (
      |system_id                      STRING,
      |language                       STRING,
      |name                           STRING,
      |operator                       STRING,
      |url                            STRING,
      |phone_number                   STRING,
      |email                          STRING,
      |timezone                       STRING
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/vasu/final_project/ext_system_information'
      |TBLPROPERTIES (
      | "skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  println("ext_system_information TABLE was CREATED")

}
