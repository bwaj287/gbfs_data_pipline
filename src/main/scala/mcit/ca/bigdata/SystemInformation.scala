package mcit.ca.bigdata

case class SystemInformation(
                              system_id:String,
                              language:String,
                              name:String,
                              operator:String,
                              url:String,
                              phone_number:String,
                              email:String,
                              timezone:String
                            )

case class SystemInformationSchema(
                                    last_updated:String,
                                    ttl:String,
                                    data:SystemInformation
                                  )
