package codec

import java.nio.ByteBuffer

import io.DecoderException

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
object MqttMessage {

  trait MessageType
  object MessageType{
    def getMessageType(value: Int):MessageType = {
      value match{
        case 1 => CONNECT
        case 2 => CONNACK
        case 3 => PUBLISH
        case 4 => PUBACK
        case 5 => PUBREC
        case 6 => PUBREL
        case 7 => PUBCOMP
        case 8 => SUBSCRIBE
        case 9 => SUBACK
        case 10 => UNSUBSCRIBE
        case 11 => UNSUBACK
        case 12 => PINGREQ
        case 13 => PINGRESP
        case 14 => DISCONNECT
        case _ => throw new DecoderException(s"wrong message type $value")
      }
    }
    def value(messageType: MessageType):Int = {
      messageType match{
        case CONNECT => 1
        case CONNACK => 2
        case PUBLISH => 3
        case PUBACK => 4
        case PUBREC => 5
        case PUBREL=> 6
        case PUBCOMP => 7
        case SUBSCRIBE => 8
        case SUBACK => 9
        case UNSUBSCRIBE => 10
        case UNSUBACK => 11
        case PINGREQ => 12
        case PINGRESP => 13
        case DISCONNECT => 14
      }
    }
  }
  case object CONNACK  extends MessageType
  case object CONNECT extends MessageType
  case object DISCONNECT extends MessageType
  case object PINGREQ extends MessageType
  case object PINGRESP extends MessageType
  case object PUBACK extends MessageType
  case object PUBCOMP extends MessageType
  case object PUBLISH extends MessageType
  case object PUBREC extends MessageType
  case object PUBREL extends MessageType
  case object SUBACK extends MessageType
  case object SUBSCRIBE extends MessageType
  case object UNSUBACK extends MessageType
  case object UNSUBSCRIBE extends MessageType

  sealed trait QoS
  object QoS {
    def getQos(value: Int): QoS = {
      value match {
        case 0 => AT_LEAST_ONCE
        case 1 => AT_MOST_ONCE
        case 2 => EXACTLY_ONCE
        case 3 => FAILURE
        case _ => throw new DecoderException(s"wrong qos $value")
      }
    }

    def value(qoS: QoS): Int = {
      qoS match {
        case AT_LEAST_ONCE => 0
        case AT_MOST_ONCE => 1
        case EXACTLY_ONCE => 2
        case FAILURE => 3
      }
    }
  }
  case object AT_LEAST_ONCE extends QoS
  case object AT_MOST_ONCE extends QoS
  case object EXACTLY_ONCE extends QoS
  case object FAILURE extends QoS

  case class FixedHeader(messageType: MessageType,isDup : Boolean, qos: QoS, isRetain: Boolean, remainingLength: Int)

  trait DecodeResult
  object SUCCESS extends DecodeResult
  object UNFINISHED extends DecodeResult

  class Message(fixedHeader: FixedHeader, variableHeader: Any, payload: Any, decodeResult: DecodeResult){
    def this(fixedHeader: FixedHeader) = this(fixedHeader, null, null, null)
    def this(fixedHeader: FixedHeader, variableHeader: Any) = this(fixedHeader, variableHeader, null, null)
    def this(fixedHeader: FixedHeader, variableHeader: Any, payload: Any) = this(fixedHeader, variableHeader, payload, null)
    def getFixedHeader = fixedHeader
    def getVariableHeader = variableHeader
    def getPayload = payload
    override def toString = s"Message(fixedHeadr=$fixedHeader, variableHeader=$variableHeader,payload$payload decodeResult $decodeResult)"
  }

  sealed trait ConnectReturnCode
  object ConnectReturnCode{
    def getReturnCode(value: Int):ConnectReturnCode ={
      value match {
        case 0 => CONNECTION_ACCEPTED
        case 1 => CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION
        case 2 => CONNECTION_REFUSED_IDENTIFIER_REJECTED
        case 3 => CONNECTION_REFUSED_SERVER_UNAVAILABLE
        case 4 => CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD
        case 5 => CONNECTION_REFUSED_NOT_AUTHORIZED
        case _ => throw new DecoderException(s"bad connect return code $value")
      }
    }

    def value (value: ConnectReturnCode):Int ={
      value match {
        case CONNECTION_ACCEPTED => 0
        case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION => 1
        case CONNECTION_REFUSED_IDENTIFIER_REJECTED => 2
        case CONNECTION_REFUSED_SERVER_UNAVAILABLE => 3
        case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD => 4
        case CONNECTION_REFUSED_NOT_AUTHORIZED => 5
      }
    }
  }
  case object CONNECTION_ACCEPTED extends ConnectReturnCode
  case object CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD extends ConnectReturnCode
  case object CONNECTION_REFUSED_IDENTIFIER_REJECTED extends ConnectReturnCode
  case object CONNECTION_REFUSED_NOT_AUTHORIZED extends ConnectReturnCode
  case object CONNECTION_REFUSED_SERVER_UNAVAILABLE extends ConnectReturnCode
  case object CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION extends ConnectReturnCode

  case class ConnAckVariableHeader(connectReturnCode: ConnectReturnCode, sessionPresent: Boolean)
  case class ConnAckMessage(fixedHeader: FixedHeader,  variableHeader: ConnAckVariableHeader)extends Message(fixedHeader, variableHeader, null, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
  }

  case class ConnectVariableHeader(name: String, version:Int, hasUserName: Boolean, hasPassword: Boolean,
                                   isWillRetain: Boolean,  willQos: Int, isWillFlag: Boolean,
                                   isCleanSession: Boolean, keepAliveTimeSeconds: Int)

  case class ConnectPayload(clientIdentifier: String, willTopic: String, willMessage:String ,userName:String, password: String )
  case class ConnectMessage(fixedHeader: FixedHeader, variableHeader: ConnectVariableHeader,
                            payload: ConnectPayload) extends Message(fixedHeader, variableHeader, payload, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
    override def getPayload = payload
  }

  case class MessageIdVariableHeader(msgId: Int){
    def messageId():Int=msgId
  }
  object MessageIdVariableHeader{
    def from(mId: Int): MessageIdVariableHeader={
       MessageIdVariableHeader(mId)
    }
  }
  case class PubAckMessage( fixedHeader: FixedHeader,
                            variableHeader: MessageIdVariableHeader) extends Message(fixedHeader, variableHeader, null, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
  }

  case class PublishVariableHeader(topicName : String, messageId : Int)
  case class PublishMessage( fixedHeader: FixedHeader,  variableHeader: PublishVariableHeader, payload: ByteBuffer) extends Message(fixedHeader, variableHeader, payload, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
    override def getPayload = payload
  }
  case class SubAckPayload(grantedQoS: List[Int]){
    require(grantedQoS != null)
    def this(grantedQoS:Array[Int]) = this(grantedQoS.toList)
    def grantedQos():List[Int] = grantedQoS
  }
  case class SubAckMessage( fixedHeader: FixedHeader,  variableHeader: MessageIdVariableHeader,
                            payload: SubAckPayload) extends Message(fixedHeader, variableHeader,payload,null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
    override def getPayload = payload
  }
  case class TopicSubscription(topicFilter: String, qoS: QoS)
  case class SubscriptionPayload(topicSubscriptions: List[TopicSubscription])
  case class SubscriptionMessage(fixedHeader: FixedHeader,  variableHeader: MessageIdVariableHeader,
                                 payload: SubscriptionPayload) extends Message(fixedHeader, variableHeader, payload, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
    override def getPayload = payload
  }

  case class UnsubAckMessage(fixedHeader: FixedHeader, variableHeader: MessageIdVariableHeader) extends Message(fixedHeader, variableHeader, null, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
  }
  case class UnsubscribePayload(topics: List[String])
  case class UnsubscribeMessage(fixedHeader: FixedHeader,  variableHeader: MessageIdVariableHeader,
                                 payload: UnsubscribePayload) extends Message(fixedHeader, variableHeader, payload, null){
    override def getFixedHeader = fixedHeader
    override def getVariableHeader = variableHeader
    override def getPayload = payload
  }

}
