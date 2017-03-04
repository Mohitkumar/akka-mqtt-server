package codec

/**
  * Created by Mohit Kumar on 2/19/2017.
  */
sealed trait MqttVersion{
  def  protocolName: String
  def protocolLevel:Byte
  def protocolNameByte:Array[Byte] = {
    protocolName.getBytes("UTF-8")
  }

}
object MqttVersion{
  def fromProtocolNameAndLevel(protocolName: String, protocolLevel:Byte): MqttVersion ={
    if(MQTT_3_1.protocolName == protocolName && MQTT_3_1.protocolLevel == protocolLevel){
      return MQTT_3_1
    }
    if(MQTT_3_1_1.protocolName == protocolName && MQTT_3_1_1.protocolLevel == protocolLevel){
      return MQTT_3_1_1
    }
    throw new UnAcceptableProtocolException(s"protocol name $protocolName or/and level $protocolLevel unacceptable")
  }
}
case object MQTT_3_1  extends MqttVersion{
  override def protocolName = "MQIsdp"
  override def protocolLevel = Byte.box(3)
}
case object MQTT_3_1_1 extends MqttVersion{
  override def protocolName = "MQTT"
  override def protocolLevel = Byte.box(4)
}

