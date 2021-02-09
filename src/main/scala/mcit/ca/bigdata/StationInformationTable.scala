package mcit.ca.bigdata

class StationInformationTable {
  val stringStationInformationTable =
    """
      |CREATE EXTERNAL TABLE winter2020_vasu.string_station_information
      |(textcol string)
      |STORED AS TEXTFILE
      |LOCATION 'hdfs://quickstart.cloudera:8020/user/winter2020/vasu/finalproject/station_information'
      |""".stripMargin

  val jsonStationInformationTable =
    """
      |CREATE EXTERNAL TABLE winter2020_vasu.json_station_information
      |(json_body string)
      |STORED AS TEXTFILE
      |""".stripMargin

  val jsonStationInformationInsertion =
    """
      |INSERT OVERWRITE TABLE winter2020_vasu.json_station_information
      |SELECT CONCAT_WS(' ',COLLECT_LIST(textcol)) AS singlelineJSON
      |FROM (SELECT INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE, textcol
      |FROM winter2020_vasu.string_station_information
      |DISTRIBUTE BY INPUT__FILE__NAME
      |SORT BY BLOCK__OFFSET__INSIDE__FILE) x
      |GROUP BY INPUT__FILE__NAME
      |""".stripMargin

  val gjoStaionInformationTable = " CREATE TABLE winter2020_vasu.gjo_station_information AS SELECT " +
    "split(get_json_object(c.json_body,'$.data.stations.station_id'),\",\") as id,  " +
      "split(get_json_object(c.json_body,'$.data.stations.name'),\",\") as name,  " +
      "split(get_json_object(c.json_body,'$.data.stations.short_name'),\",\") as short_name, " +
      "split(get_json_object(c.json_body,'$.data.stations.lat'),\",\") as lat, " +
      "split(get_json_object(c.json_body,'$.data.stations.lon'),\",\") as lon, " +
      "split(get_json_object(c.json_body,'$.data.stations.region_id'),\",\") as region_id, " +
      "split(get_json_object(c.json_body,'$.data.stations.capacity'),\",\") as capacity " +
      "FROM winter2020_vasu.json_station_information c "

  val tableStationIdName =
    """
      |CREATE table winter2020_vasu.sid_name AS
      |SELECT posi,split(sid,'\"')[1] as sid,split(sname,'\"')[1] as sname
      |FROM winter2020_vasu.gjo_station_information k
      |LATERAL VIEW posexplode(id) k AS posi, sid
      |LATERAL VIEW posexplode(name) K AS posn, sname
      |where posi = posn
      |""".stripMargin

  val tableLatLon =
    """
      |CREATE table winter2020_vasu.slat_lon AS
      |SELECT posl,split(slat,'\"')[0] as latitude,split(slon,'\"')[0] as longtitude
      |FROM winter2020_vasu.gjo_station_information k
      |LATERAL VIEW posexplode(lat) k AS posl, slat
      |LATERAL VIEW posexplode(lon) K AS posln, slon
      |where posl = posln
      |""".stripMargin

  val tableSnameCapacity =
    """
      |CREATE table winter2020_vasu.sname_cap AS
      |SELECT possn,split(ssname,'\"')[1] as ShortName,split(cap,'\"')[0] as Capacity
      |FROM winter2020_vasu.gjo_station_information k
      |LATERAL VIEW posexplode(short_name) k AS possn, ssname
      |LATERAL VIEW posexplode(capacity) k AS posc, cap
      |where possn = posc
      |""".stripMargin

  val tableRegId =
    """
      |create table winter2020_vasu.rid as
      |SELECT row_number() over() as rownum ,split(rm,'\"')[1] as region_id
      |FROM winter2020_vasu.gjo_station_information k
      |LATERAL VIEW posexplode(region_id) k AS posb, rm
      |WHERE posb%2 != 0
      |""".stripMargin

  val extStatInfo =
    """
      |CREATE table winter2020_vasu.station_information as SELECT cast(a.sid as INT) as station_Id,a.sname as sName,
      |b.shortname as short_name,b.capacity as Capacity,
      |c.latitude as lat,c.longtitude as lon,
      |d.region_id as region_id
      |FROM winter2020_vasu.sid_name a
      |JOIN winter2020_vasu.sname_cap b on a.posi =b.possn
      |JOIN winter2020_vasu.slat_lon c on c.posl= b.possn
      |JOIN winter2020_vasu.rid d on d.rownum = c.posl
      |""".stripMargin

}
