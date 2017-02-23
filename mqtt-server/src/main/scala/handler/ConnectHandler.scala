package handler

import akka.actor.{ActorLogging, Actor}
import akka.io.Tcp.Write
import akka.util.ByteString
import codec.MqttMessage._
import codec.{MQTT_3_1_1, MQTT_3_1, MqttVersion}
import io.{UnAcceptableProtocolException, Channel, Encoder}

/**
  * Created by Mohit Kumar on 2/23/2017.
  */
class ConnectHandler(channel :Channel) extends Actor with ActorLogging{
  import ConnectHandler._
  def receive = {
    case message:Message =>{
      val fixedHeader = message.getFixedHeader
      val variableHeader = message.getVariableHeader.asInstanceOf[ConnectVariableHeader]
      val payload = message.getPayload.asInstanceOf[ConnectPayload]
      try {
        MqttVersion.fromProtocolNameAndLevel(variableHeader.name, variableHeader.version.toByte)
      }catch {
        case e: UnAcceptableProtocolException => sender ! Write(createConnAckFailureUnacceptableProtocol())
      }

      if(payload.clientIdentifier == null  || payload.clientIdentifier.isEmpty){
        sender ! Write(createConnAckFailureInvalidClientId())
        channel.close()
      }
      if(!variableHeader.hasUserName && variableHeader.hasPassword){
        sender ! Write(createConnAckFailureBadUserPassword())
        channel.close()
      }
      if(variableHeader.hasUserName && payload.userName == null){
        sender ! Write(createConnAckFailureBadUserPassword())
        channel.close()
      }
      if(variableHeader.hasPassword && payload.password == null){
        sender ! Write(createConnAckFailureBadUserPassword())
        channel.close()
      }
      if(!authorize(payload.userName,payload.password)){
        sender ! Write(createConnAckFailureNotAuthorised())
        channel.close()
      }
    }
  }
}
object ConnectHandler{
  def createConnAckFailureUnacceptableProtocol():ByteString = {
    val fixedHeader = FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,2)
    val variableHeader  = ConnAckVariableHeader(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,false);
    val connAckMessage = ConnAckMessage(fixedHeader,variableHeader)
    Encoder.encode(connAckMessage)
  }

  def createConnAckFailureInvalidClientId():ByteString = {
    val fixedHeader = FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,2)
    val variableHeader  = ConnAckVariableHeader(CONNECTION_REFUSED_IDENTIFIER_REJECTED,false);
    val connAckMessage = ConnAckMessage(fixedHeader,variableHeader)
    Encoder.encode(connAckMessage)
  }

  def createConnAckFailureBadUserPassword():ByteString = {
    val fixedHeader = FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,2)
    val variableHeader  = ConnAckVariableHeader(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,false);
    val connAckMessage = ConnAckMessage(fixedHeader,variableHeader)
    Encoder.encode(connAckMessage)
  }

  def createConnAckFailureNotAuthorised():ByteString = {
    val fixedHeader = FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,2)
    val variableHeader  = ConnAckVariableHeader(CONNECTION_REFUSED_NOT_AUTHORIZED,false);
    val connAckMessage = ConnAckMessage(fixedHeader,variableHeader)
    Encoder.encode(connAckMessage)
  }
  def authorize(userName:String,  password:String): Boolean ={
    true
  }
}
