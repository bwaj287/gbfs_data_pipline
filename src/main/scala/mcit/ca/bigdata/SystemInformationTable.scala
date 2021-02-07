package mcit.ca.bigdata

class SystemInformationTable {
  val stringSysInfoTable =
    """
      |CREATE EXTERNAL TABLE winter2020_vasu.string_system_information (textcol string)
      |STORED AS TEXTFILE
      |LOCATION 'hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/system_information'
      |""".stripMargin

  val jsonSysInfoTable =
    """
      |CREATE EXTERNAL TABLE winter2020_vasu.fj_system_information (json_body string)
      |STORED AS TEXTFILE
      |""".stripMargin

  val jsonSysInfoInsertion =
    """
      |INSERT OVERWRITE TABLE winter2020_vasu.fj_system_information
      |SELECT CONCAT_WS(' ',COLLECT_LIST(textcol)) AS singlelineJSON
      |FROM (SELECT INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE, textcol
      |FROM winter2020_vasu.string_system_information
      |DISTRIBUTE BY INPUT__FILE__NAME
      |SORT BY BLOCK__OFFSET__INSIDE__FILE) x
      |GROUP BY INPUT__FILE__NAME
      |""".stripMargin

  val extSysInfo =
    """
      |create table winter2020_vasu.system_information as select
      |get_json_object(c.json_body,'$.data.system_id') as system_id,
      |get_json_object(c.json_body,'$.data.language') as language,
      |get_json_object(c.json_body,'$.data.url') as url,
      |get_json_object(c.json_body,'$.data.timezone') as timezone
      |from winter2020_vasu.fj_system_information c
      |""".stripMargin

}
