package codec

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
object MqttMessage {

  trait MessageType
  object MessageType{
    def getMessageType(value: Int):MessageType = {
      value match{
        case 0 => CONNACK
        case 1 => CONNECT
        case 2 => DISCONNECT
        case 3 => PINGREQ
        case 4 => PINGRESP
        case 5 => PUBACK
        case 6 => PUBCOMP
        case 7 => PUBLISH
        case 8 => PUBREC
        case 9 => SUBACK
        case 10 => SUBSCRIBE
        case 11 => UNSUBACK
        case 12 => UNSUBSCRIBE
        case _ => throw new IllegalArgumentException("wrong message type")
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

  trait QoS
  case object AT_LEAST_ONCE extends QoS
  case object AT_MOST_ONCE extends QoS
  case object EXACTLY_ONCE extends QoS
  case object FAILURE extends QoS

  case class FixedHeader(messageType: MessageType,isDup : Boolean, qos: QoS, isRetain: Boolean, remainingLength: Int)

  trait DecodeResult
  object SUCCESS extends DecodeResult
  object UNFINISHED extends DecodeResult

  case class Message(fixedHeader: FixedHeader, variableHeader: Any, payload: Any, decodeResult: DecodeResult){
    def this(fixedHeader: FixedHeader) = this(fixedHeader, null, null, null)
    def this(fixedHeader: FixedHeader, variableHeader: Any) = this(fixedHeader, variableHeader, null, null)
    def this(fixedHeader: FixedHeader, variableHeader: Any, payload: Any) = this(fixedHeader, variableHeader, payload, null)

  }

  trait ConnectReturnCode
  case object CONNECTION_ACCEPTED extends ConnectReturnCode
  case object CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD extends ConnectReturnCode
  case object CONNECTION_REFUSED_IDENTIFIER_REJECTED extends ConnectReturnCode
  case object CONNECTION_REFUSED_NOT_AUTHORIZED extends ConnectReturnCode
  case object CONNECTION_REFUSED_SERVER_UNAVAILABLE extends ConnectReturnCode
  case object CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION extends ConnectReturnCode

  case class ConnAckVariableHeader(connectReturnCode: ConnectReturnCode, sessionPresent: Boolean)
  case class ConnAckMessage(override var fixedHeader: FixedHeader, override var variableHeader: ConnAckVariableHeader)extends Message(fixedHeader, variableHeader, null, null)

  case class ConnectVariableHeader(name: String, version:Int, hasUserName: Boolean, hasPassword: Boolean,
                                   isWillRetain: Boolean,  willQos: Int, isWillFlag: Boolean,
                                   isCleanSession: Boolean, keepAliveTimeSeconds: Int)

  case class ConnectPayload(clientIdentifier: String, willTopic: String, willMessage:String ,userName:String, password: String )
  case class ConnectMessage(override var fixedHeader: FixedHeader, override var variableHeader: ConnectVariableHeader,
                            override  var payload: ConnectPayload) extends Message(fixedHeader, variableHeader, payload, null)

  case class MessageIdVariableHeader(msgId: Int){
    def messageId():Int=msgId
  }
  object MessageIdVariableHeader{
    def from(mId: Int): MessageIdVariableHeader={
       MessageIdVariableHeader(mId)
    }
  }
  case class PubAckMessage(override var fixedHeader: FixedHeader,
                           override var variableHeader: MessageIdVariableHeader) extends Message(fixedHeader, variableHeader, null, null)

  case class PublishVariableHeader(topicName : String, messageId : String)
  case class PublishMessage(override var fixedHeader: FixedHeader, override var variableHeader: PublishVariableHeader, override  var payload: String) extends Message(fixedHeader, variableHeader, payload, null)
  case class SubAckPayload(grantedQoS: List[Int]){
    require(grantedQoS != null)
    def this(grantedQoS:Array[Int]) = this(grantedQoS.toList)
    def grantedQos():List[Int] = grantedQoS
  }
  case class SubAckMessage(override var fixedHeader: FixedHeader, override var variableHeader: MessageIdVariableHeader,
                           override var payload: SubAckPayload) extends Message(fixedHeader, variableHeader,payload,null)
  case class TopicSubscription(topicFilter: String, qoS: QoS)
  case class SubscriptionPayload(topicSubscriptions: List[TopicSubscription])
  case class SubscriptionMessage(override  var fixedHeader: FixedHeader, override var variableHeader: MessageIdVariableHeader,
                                 override  var payload: SubscriptionPayload) extends Message(fixedHeader, variableHeader, payload, null)

  case class UnsubAckMessage(override  var fixedHeader: FixedHeader, override  var variableHeader: MessageIdVariableHeader) extends Message(fixedHeader, variableHeader, null, null)
  case class UnsubscribePayload(topics: List[String])
  case class UnsubscribeMessage(override  var fixedHeader: FixedHeader, override var variableHeader: MessageIdVariableHeader,
                                override var payload: UnsubscribePayload) extends Message(fixedHeader, variableHeader, payload, null)

}
