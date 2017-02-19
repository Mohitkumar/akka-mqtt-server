package io

import java.nio.ByteBuffer

import akka.util.ByteString
import codec.MqttMessage._
import codec.{MqttMessage, MQTT_3_1, MQTT_3_1_1, MqttVersion}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Decoder {
  def decode(byteString: ByteString): Message ={
    val fixedHeader = decodeFixedHeader(byteString)
    var remainingLength = fixedHeader.remainingLength
    if(remainingLength > Decoder.DEFAULT_MAX_BYTES_IN_MESSAGE){
      throw new DecoderException(s"too large message: $remainingLength bytes")
    }
    val decodedVariableHeader = readVariableHeader(byteString,fixedHeader)
    val variableHeader = decodedVariableHeader.value
    remainingLength -= decodedVariableHeader.numberOfByteConsumed
    val decodedPayload = decodePayload(byteString,fixedHeader.messageType,remainingLength,variableHeader)
    val payload = decodedPayload.value
    remainingLength -= decodedPayload.numberOfByteConsumed
    if(remainingLength != 0){
      throw new DecoderException(s"non-zero remaining payload bytes: $remainingLength (${fixedHeader.messageType})");
    }
    new Message(fixedHeader,variableHeader,payload)
  }

  def decodeFixedHeader(byteString: ByteString):FixedHeader = {
    val buffer = byteString.asByteBuffer
    val b1 = buffer.get()
    val msgType = MessageType.getMessageType(b1 >> 4)
    val dupFlag = b1.&(0x08) == 0x08
    val qosLevel = (b1 & 0x06) >> 1
    val retain = (b1 & 0x01) != 0
    var remainingLength = 0;
    var multiplier = 1;
    var digit: Short = 0
    var loops = 0;
    do {
      digit = buffer.get();
      remainingLength += (digit & 127) * multiplier;
      multiplier *= 128;
      loops += 1;
    } while ((digit & 128) != 0 && loops < 4);

    // MQTT protocol limits Remaining Length to 4 bytes
    if (loops == 4 && (digit & 128) != 0) {
      throw new DecoderException("remaining length exceeds 4 digits (" + msgType + ')');
    }
    val decodedFixedHeader = FixedHeader(msgType, dupFlag, QoS.getQos(qosLevel), retain, remainingLength);
    return validateFixedHeader(resetUnusedFields(decodedFixedHeader))
  }

  def readVariableHeader(byteString: ByteString, fixedHeader: FixedHeader): Result[_] ={
    fixedHeader.messageType match {
      case CONNECT => decodeConnectionVariableHeader(byteString)
      case CONNACK => decodeConnAckVariableHeader(byteString)
      case SUBSCRIBE | UNSUBSCRIBE | SUBACK | UNSUBACK | PUBACK | PUBREC | PUBCOMP | PUBREL => decodeMessageIdVariableHeader(byteString)
      case PUBLISH => decodePublishVariableHeader(byteString,fixedHeader)
      case PINGREQ  | PINGRESP | DISCONNECT => ResultObj(null,0)
      case _ =>  ResultObj(null,0)
    }
  }
  def decodeConnectionVariableHeader(byteString: ByteString): Result[ConnectVariableHeader]={
      val buffer = byteString.asByteBuffer
      val protocolStrResult = decodeString(byteString)
      var byteConsumed = protocolStrResult.numberOfByteConsumed
      val protoLevel = buffer.get
      byteConsumed += 1
      val mqttVersion = MqttVersion.fromProtocolNameAndLevel(protocolStrResult.value,protoLevel)
      val b1 = buffer.get
      val keepAlive = decodeMsbLsb(byteString)
      byteConsumed += keepAlive.numberOfByteConsumed
      val hasUserName = (b1 & 0x80) == 0x80
      val hasPassword = (b1 & 0x40) == 0x40;
      val willRetain = (b1 & 0x20) == 0x20;
      val willQos = (b1 & 0x18) >> 3;
      val willFlag = (b1 & 0x04) == 0x04;
      val cleanSession = (b1 & 0x02) == 0x02;
      if(mqttVersion == MQTT_3_1_1){
        val zeroReservedFlag = (b1 & 0x01) == 0x0
        if(!zeroReservedFlag){
          throw new DecoderException(s"non-zero reserved flag in protocol $mqttVersion")
        }
      }
    val connectVariableHeader = ConnectVariableHeader(mqttVersion.protocolName,mqttVersion.protocolLevel,hasUserName,hasPassword,willRetain,willQos,willFlag,cleanSession,keepAlive.value)
    ResultObj(connectVariableHeader,byteConsumed)
  }

  def decodeConnAckVariableHeader(byteString: ByteString): Result[ConnAckVariableHeader] ={
    val buffer = byteString.asByteBuffer
    val sessionPresent = (buffer.get & 0x01) == 0x01
    val returnCode =  buffer.get
    val byteConsumed = 2
    val connAckVariableHeader = ConnAckVariableHeader(ConnectReturnCode.getReturnCode(returnCode),sessionPresent)
    ResultObj(connAckVariableHeader,byteConsumed)
  }
  def decodeMessageIdVariableHeader(byteString: ByteString):Result[MessageIdVariableHeader] = {
    val messageId = decodeMessageId(byteString)
    ResultObj[MessageIdVariableHeader](MessageIdVariableHeader.from(messageId.value),messageId.numberOfByteConsumed)
  }

  def decodePublishVariableHeader(byteString: ByteString, mqttFixedHeader:FixedHeader):Result[PublishVariableHeader] = {
    val decodedTopic = decodeString(byteString)
    if (!isValidPublishTopicName(decodedTopic.value)) {
      throw new DecoderException("invalid publish topic name: " + decodedTopic.value + " (contains wildcards)");
    }
    var numberOfBytesConsumed = decodedTopic.numberOfByteConsumed;
    var messageId = -1;
    if (QoS.value(mqttFixedHeader.qos) > 0) {
      val decodedMessageId = decodeMessageId(byteString);
      messageId = decodedMessageId.value;
      numberOfBytesConsumed += decodedMessageId.numberOfByteConsumed;
    }
    val mqttPublishVariableHeader = PublishVariableHeader(decodedTopic.value, messageId);
    ResultObj[PublishVariableHeader](mqttPublishVariableHeader, numberOfBytesConsumed);
  }

  def decodePayload(byteString: ByteString, messageType: MessageType
                    , bytesRemainingInVariablePart: Int, variableHeader:Any):Result[_] = {
    messageType match {
      case CONNECT => decodeConnectionPayload(byteString, variableHeader.asInstanceOf[ConnectVariableHeader]);

      case SUBSCRIBE => decodeSubscribePayload(byteString, bytesRemainingInVariablePart);

      case SUBACK => decodeSubackPayload(byteString, bytesRemainingInVariablePart);

      case UNSUBSCRIBE => decodeUnsubscribePayload(byteString, bytesRemainingInVariablePart);

      case PUBLISH => decodePublishPayload(byteString, bytesRemainingInVariablePart);

      case _ => ResultObj(null, 0);
    }
  }

  def decodeConnectionPayload(byteString: ByteString, mqttConnectVariableHeader:ConnectVariableHeader):Result[ConnectPayload] = {
    val decodedClientId = decodeString(byteString)
    val decodedClientIdValue = decodedClientId.value;
    val mqttVersion = MqttVersion.fromProtocolNameAndLevel(mqttConnectVariableHeader.name,
      mqttConnectVariableHeader.version.toByte);

    if (!isValidClientId(mqttVersion, decodedClientIdValue)) {
      throw new MqttIdentifierRejectedException("invalid clientIdentifier: " + decodedClientIdValue);
    }
    var numberOfBytesConsumed = decodedClientId.numberOfByteConsumed;

    var decodedWillTopic:Result[String] = null;
    var decodedWillMessage:Result[String] = null;
    if (mqttConnectVariableHeader.isWillFlag) {
      decodedWillTopic = decodeString(byteString, 0, 32767);
      numberOfBytesConsumed += decodedWillTopic.numberOfByteConsumed
      decodedWillMessage = decodeAsciiString(byteString);
      numberOfBytesConsumed += decodedWillMessage.numberOfByteConsumed;
    }
    var decodedUserName :Result[String] = null;

    var decodedPassword:Result[String] = null;
    if (mqttConnectVariableHeader.hasUserName) {
      decodedUserName = decodeString(byteString);
      numberOfBytesConsumed += decodedUserName.numberOfByteConsumed;
    }
    if (mqttConnectVariableHeader.hasPassword) {
      decodedPassword = decodeString(byteString);
      numberOfBytesConsumed += decodedPassword.numberOfByteConsumed;
    }

    val mqttConnectPayload = ConnectPayload(decodedClientId.value,
        if(decodedWillTopic != null) decodedWillTopic.value else  null,
    if(decodedWillMessage != null) decodedWillMessage.value else  null,
    if(decodedUserName != null) decodedUserName.value else  null,
    if(decodedPassword != null) decodedPassword.value else null)
    return ResultObj(mqttConnectPayload, numberOfBytesConsumed)
  }

  def decodeSubscribePayload(byteString: ByteString, bytesRemainingInVariablePart:Int):Result[SubscriptionPayload] = {
    val buffer = byteString.asByteBuffer
    val subscribeTopics = List[TopicSubscription]()
    var numberOfBytesConsumed = 0;
    while (numberOfBytesConsumed < bytesRemainingInVariablePart) {
      val decodedTopicName = decodeString(byteString)
      numberOfBytesConsumed += decodedTopicName.numberOfByteConsumed
      val qos = buffer.get & 0x03
      numberOfBytesConsumed += 1;
      subscribeTopics.+:(TopicSubscription(decodedTopicName.value, QoS.getQos(qos)))
    }
     ResultObj[SubscriptionPayload](SubscriptionPayload(subscribeTopics), numberOfBytesConsumed)
  }

  def decodeSubackPayload(byteString: ByteString, bytesRemainingInVariablePart:Int): Result[SubAckPayload] = {
    val buffer = byteString.asByteBuffer
    val grantedQos = List[Int]()
    var numberOfBytesConsumed = 0;
    while (numberOfBytesConsumed < bytesRemainingInVariablePart) {
      val qos = buffer.get & 0x03
      numberOfBytesConsumed += 1
      grantedQos.+:(qos)
    }
    ResultObj(SubAckPayload(grantedQos), numberOfBytesConsumed)
  }

  def decodeUnsubscribePayload(byteString: ByteString, bytesRemainingInVariablePart: Int) = {
    val unsubscribeTopics = List[String]()
    var numberOfBytesConsumed = 0
    while (numberOfBytesConsumed < bytesRemainingInVariablePart) {
      val decodedTopicName = decodeString(byteString)
      numberOfBytesConsumed += decodedTopicName.numberOfByteConsumed
      unsubscribeTopics.+:(decodedTopicName.value)
    }
    ResultObj(UnsubscribePayload(unsubscribeTopics), numberOfBytesConsumed)
  }

  def decodePublishPayload(byteString: ByteString, bytesRemainingInVariablePart:Int):Result[ByteBuffer]= {
    val buffer = byteString.asByteBuffer
    val resultBuf = ByteBuffer.allocate(bytesRemainingInVariablePart)
    for(i <- 0 to bytesRemainingInVariablePart){
      resultBuf.put(buffer.get)
    }
    ResultObj[ByteBuffer](resultBuf, bytesRemainingInVariablePart);
  }

  def decodeMessageId(byteString: ByteString):Result[Int]= {
    val messageId = decodeMsbLsb(byteString);
    if (!isValidMessageId(messageId.value)) {
      throw new DecoderException("invalid messageId: " + messageId.value);
    }
    messageId;
  }
  def decodeString(byteString: ByteString): Result[String] ={
    decodeString(byteString,0,Int.MaxValue)
  }
  def decodeString(byteString: ByteString, min:Int, max:Int): Result[String] = {
    val buffer = byteString.asByteBuffer
    val decodedSize = decodeMsbLsb(byteString)
    val size = decodedSize.value
    var byteConsumed = decodedSize.numberOfByteConsumed
    if(size < min || size > max){
      skipBytes(buffer,size)
      byteConsumed = byteConsumed + size
      ResultObj[String](null, byteConsumed)
    }
    val arr: Array[Byte] = new Array[Byte](size)
    buffer.get(arr,0,size)
    val s = new String(arr,"UTF-8")
    skipBytes(buffer, size)
    byteConsumed = byteConsumed + size
    ResultObj[String](s,byteConsumed)
  }

 def decodeAsciiString(byteString: ByteString):Result[String] =  {
    val result = decodeString(byteString, 0, Integer.MAX_VALUE);
    val s = result.value;
    for (i <- 0 to s.length) {
      if (s.charAt(i) > 127) {
        ResultObj(null, result.numberOfByteConsumed);
      }
    }
    ResultObj(s, result.numberOfByteConsumed);
  }

  def  decodeMsbLsb(byteString: ByteString): Result[Int] = {
    decodeMsbLsb(byteString,0,65535)
  }
  def  decodeMsbLsb(byteString: ByteString, min:Int, max:Int): Result[Int]={
    val buffer = byteString.asByteBuffer
    val msbLength = buffer.get
    val lsbLength = buffer.get
    var result = (msbLength << 8) | lsbLength
    val byteConsumed = 2
    if(result < min || result > max){
      result = -1
    }
    ResultObj(result,byteConsumed)
  }
  def resetUnusedFields(fixedHeader: FixedHeader): FixedHeader = {
    fixedHeader.messageType match {
      case CONNECT | CONNACK | PUBACK | PUBREC | PUBCOMP | SUBACK | UNSUBACK | PINGREQ | PINGRESP | DISCONNECT =>{
        if (fixedHeader.isDup || fixedHeader.qos != AT_MOST_ONCE || fixedHeader.isRetain) {
           new FixedHeader(fixedHeader.messageType, false, AT_MOST_ONCE, false, fixedHeader.remainingLength)
        }
        fixedHeader
      }
      case PUBREL | SUBSCRIBE | UNSUBSCRIBE =>{
        if(fixedHeader.isRetain){
          new FixedHeader(fixedHeader.messageType, fixedHeader.isDup, fixedHeader.qos,false,fixedHeader.remainingLength)
        }
        fixedHeader
      }
      case _ => fixedHeader
    }
  }

  def validateFixedHeader(fixedHeader: FixedHeader): FixedHeader= {
    fixedHeader.messageType match {
      case PUBREL | SUBSCRIBE | UNSUBSCRIBE => {
        if(fixedHeader.qos != AT_LEAST_ONCE){
          throw new DecoderException(s"${fixedHeader.messageType} message must have QoS 1")
        }
        fixedHeader
      }
      case _ => fixedHeader
    }
  }

  def skipBytes(byteBuffer: ByteBuffer, size:Int): Unit ={
    for(i <- 0 to size) {byteBuffer.get}
  }

  def isValidPublishTopicName(topicName :String):Boolean= {
    // publish topic name must not contain any wildcard
    for(c <- Decoder.TOPIC_WILDCARDS){
      if (topicName.indexOf(c) >= 0) {
        false;
      }
    }
    true;
  }

  def isValidMessageId(messageId: Int):Boolean = {
    messageId != 0;
  }

  def isValidClientId(mqttVersion: MqttVersion, clientId:String):Boolean = {
    if (mqttVersion == MQTT_3_1) {
      return clientId != null && clientId.length() >= Decoder.MIN_CLIENT_ID_LENGTH &&
        clientId.length() <= Decoder.MAX_CLIENT_ID_LENGTH;
    }
    if (mqttVersion == MQTT_3_1_1) {
      // In 3.1.3.1 Client Identifier of MQTT 3.1.1 specification, The Server MAY allow ClientId’s
      // that contain more than 23 encoded bytes. And, The Server MAY allow zero-length ClientId.
      return clientId != null;
    }
    throw new IllegalArgumentException(s" $mqttVersion is unknown mqtt version");
  }

}
object Decoder{
    val TOPIC_WILDCARDS:Array[Char] = Array('#', '+')
    val MIN_CLIENT_ID_LENGTH = 1;
    val MAX_CLIENT_ID_LENGTH = 23;
    val DEFAULT_MAX_BYTES_IN_MESSAGE = 8092;

    def decodeMsg(byteString: ByteString) = new Decoder().decode(byteString)
}
