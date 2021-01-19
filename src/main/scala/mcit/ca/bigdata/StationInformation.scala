package mcit.ca.bigdata

case class StationInformation(
                               station_id:String,
                               name:String,
                               short_name:String,
                               lat:String,
                               lon:String,
                               region_id:Int,
                               capacity:String
                             )

case class Data(stations:Array[StationInformation])

case class StationInformationSchema(
                                     last_updated: String,
                                     ttl:String,
                                     data:Data
                                   )

