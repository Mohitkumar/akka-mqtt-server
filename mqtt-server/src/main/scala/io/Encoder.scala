package io

import java.nio.ByteBuffer

import akka.util.ByteString
import codec.MqttMessage._
import codec.MqttVersion
import io.Util._

/**
  * Created by Mohit Kumar on 2/20/2017.
  */
class Encoder {
  def doEncode(message : Message):ByteBuffer = {
    val buffer:ByteBuffer= ByteBuffer.allocate(0)
    message.getFixedHeader.messageType match {
      case CONNECT =>
      return encodeConnectMessage(message.asInstanceOf[ConnectMessage]);

      case CONNACK =>
      return encodeConnAckMessage(message.asInstanceOf[ConnAckMessage]);

      case PUBLISH=>
      return encodePublishMessage(message.asInstanceOf[PublishMessage]);

      case SUBSCRIBE=>
      return encodeSubscribeMessage(message.asInstanceOf[SubscriptionMessage]);

      case UNSUBSCRIBE=>
      return encodeUnsubscribeMessage(message.asInstanceOf[UnsubscribeMessage]);

      case SUBACK=>
      return encodeSubAckMessage(message.asInstanceOf[SubAckMessage]);

      case UNSUBACK | PUBACK | PUBREC | PUBREL | PUBCOMP => encodeMessageWithOnlySingleByteFixedHeaderAndMessageId(message);

      case PINGREQ | PINGRESP | DISCONNECT => encodeMessageWithOnlySingleByteFixedHeader(message);

      case _ =>
      throw new IllegalArgumentException(
        "Unknown message type: " + MessageType.value(message.getFixedHeader.messageType));
    }
  }

  def encodeConnectMessage(message: ConnectMessage): ByteBuffer = {
    var payloadBufferSize = 0;
    val mqttFixedHeader = message.getFixedHeader
    val variableHeader = message.getVariableHeader
    val payload = message.getPayload
    val mqttVersion = MqttVersion.fromProtocolNameAndLevel(variableHeader.name, variableHeader.version.toByte);
    // Client id
    val clientIdentifier = payload.clientIdentifier;
    if (!isValidClientId(mqttVersion, clientIdentifier)) {
      throw new MqttIdentifierRejectedException("invalid clientIdentifier: " + clientIdentifier);
    }
    val clientIdentifierBytes = encodeStringUtf8(clientIdentifier);
    payloadBufferSize += 2 + clientIdentifierBytes.length;

    // Will topic and message
    val willTopic = payload.willTopic
    val willTopicBytes = if(willTopic != null) encodeStringUtf8(willTopic) else Array[Byte]();
    val willMessage = payload.willMessage;
    val willMessageBytes = if(willMessage != null) encodeStringUtf8(willMessage) else Array[Byte]();
    if (variableHeader.isWillFlag) {
      payloadBufferSize += 2 + willTopicBytes.length;
      payloadBufferSize += 2 + willMessageBytes.length;
    }
    val userName = payload.userName
    val userNameBytes = if(userName != null) encodeStringUtf8(userName) else Array[Byte]()
    if (variableHeader.hasUserName) {
      payloadBufferSize += 2 + userNameBytes.length
    }

    val password = payload.password
    val passwordBytes = if(password != null) encodeStringUtf8(password) else Array[Byte]();
    if (variableHeader.hasPassword) {
      payloadBufferSize += 2 + passwordBytes.length;
    }

    // Fixed header
    val protocolNameBytes = mqttVersion.protocolNameByte;
    val variableHeaderBufferSize = 2 + protocolNameBytes.length + 4;
    val variablePartSize = variableHeaderBufferSize + payloadBufferSize;
    val fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);
    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variablePartSize);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte);
    writeVariableLengthInt(buf, variablePartSize);

    buf.putShort(protocolNameBytes.length.toShort);
    buf.put(protocolNameBytes);

    buf.put(variableHeader.version.toByte);
    buf.put(getConnVariableHeaderFlag(variableHeader).toByte);
    buf.putShort(variableHeader.keepAliveTimeSeconds.toShort);

    // Payload
    buf.putShort(clientIdentifierBytes.length.toShort);
    buf.put(clientIdentifierBytes, 0, clientIdentifierBytes.length);
    if (variableHeader.isWillFlag) {
      buf.putShort(willTopicBytes.length.toShort);
      buf.put(willTopicBytes, 0, willTopicBytes.length);
      buf.putShort(willMessageBytes.length.toShort);
      buf.put(willMessageBytes, 0, willMessageBytes.length);
    }
    if (variableHeader.hasUserName) {
      buf.putShort(userNameBytes.length.toShort);
      buf.put(userNameBytes, 0, userNameBytes.length);
    }
    if (variableHeader.hasPassword) {
      buf.putShort(passwordBytes.length.toShort);
      buf.put(passwordBytes, 0, passwordBytes.length);
    }
    return buf;
  }

  def getConnVariableHeaderFlag(variableHeader: ConnectVariableHeader): Int = {
    var flagByte = 0;
    if (variableHeader.hasUserName) {
      flagByte |= 0x80;
    }
    if (variableHeader.hasPassword) {
      flagByte |= 0x40;
    }
    if (variableHeader.isWillRetain) {
      flagByte |= 0x20;
    }
    flagByte |= (variableHeader.willQos & 0x03) << 3;
    if (variableHeader.isWillFlag) {
      flagByte |= 0x04;
    }
    if (variableHeader.isCleanSession) {
      flagByte |= 0x02;
    }
    return flagByte;
  }

  def encodeConnAckMessage(message: ConnAckMessage): ByteBuffer =  {
    val buf = ByteBuffer.allocate(4);
    buf.put(getFixedHeaderByte1(message.fixedHeader).toByte);
    buf.put(2.toByte);
    buf.put(if(message.variableHeader.sessionPresent) 0x01.toByte else 0x00.toByte);
    buf.put(ConnectReturnCode.value(message.variableHeader.connectReturnCode).toByte);
    return buf;
  }

  def encodeSubscribeMessage(message : SubscriptionMessage) : ByteBuffer = {
    val variableHeaderBufferSize = 2;
    var payloadBufferSize = 0;
    val mqttFixedHeader = message.fixedHeader
    val variableHeader = message.variableHeader
    val payload = message.payload
    for(topic <- payload.topicSubscriptions) {
      val topicName = topic.topicFilter;
      val topicNameBytes = encodeStringUtf8(topicName);
      payloadBufferSize += 2 + topicNameBytes.length;
      payloadBufferSize += 1;
    }

    val variablePartSize = variableHeaderBufferSize + payloadBufferSize;
    val fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variablePartSize);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte);
    writeVariableLengthInt(buf, variablePartSize);
    // Variable Header
    val messageId = variableHeader.messageId();
    buf.putShort(messageId.toShort);

    // Payload
    for(topic <- payload.topicSubscriptions) {
      val topicName = topic.topicFilter;
      val topicNameBytes = encodeStringUtf8(topicName);
      buf.putShort(topicNameBytes.length.toShort);
      buf.put(topicNameBytes, 0, topicNameBytes.length);
      buf.put(QoS.value(topic.qoS).toByte);
    }
    return buf;
  }

  def encodeUnsubscribeMessage(message: UnsubscribeMessage): ByteBuffer = {
    val variableHeaderBufferSize = 2;
    var payloadBufferSize = 0;

    val mqttFixedHeader = message.getFixedHeader;
    val variableHeader = message.variableHeader;
    val payload = message.payload;

    for(topicName <- payload.topics){
      val topicNameBytes = encodeStringUtf8(topicName);
      payloadBufferSize += 2 + topicNameBytes.length;
    }

    val variablePartSize = variableHeaderBufferSize + payloadBufferSize;
    val fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variablePartSize);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte);
    writeVariableLengthInt(buf, variablePartSize);

    // Variable Header
    val messageId = variableHeader.messageId();
    buf.putShort(messageId.toShort);

    // Payload
    for(topicName <- payload.topics) {
      val topicNameBytes = encodeStringUtf8(topicName);
      buf.putShort(topicNameBytes.length.toByte);
      buf.put(topicNameBytes, 0, topicNameBytes.length);
    }
    return buf;
  }

  def  encodeSubAckMessage(message: SubAckMessage):ByteBuffer = {
    val variableHeaderBufferSize = 2;
    val payloadBufferSize = message.payload.grantedQos().size;
    val variablePartSize = variableHeaderBufferSize + payloadBufferSize;
    val  fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);
    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variablePartSize);
    buf.put(getFixedHeaderByte1(message.fixedHeader).toByte);
    writeVariableLengthInt(buf, variablePartSize);
    buf.putShort(message.variableHeader.messageId().toShort);
    for(qos <- message.payload.grantedQos()){
      buf.put(qos.toByte);
    }
    return buf;
  }

  def encodePublishMessage(message : PublishMessage):ByteBuffer = {
    val mqttFixedHeader = message.fixedHeader;
    val variableHeader = message.variableHeader;
    val payload = message.payload.duplicate();

    val topicName = variableHeader.topicName;
    val topicNameBytes = encodeStringUtf8(topicName);
    val qos = if(QoS.value(mqttFixedHeader.qos)> 0) 2 else 0
    val variableHeaderBufferSize = 2 + topicNameBytes.length + qos;
    val  payloadBufferSize = payload.remaining();
    val variablePartSize = variableHeaderBufferSize + payloadBufferSize;
    val fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variablePartSize);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte);
    writeVariableLengthInt(buf, variablePartSize);
    buf.putShort(topicNameBytes.length.toShort);
    buf.put(topicNameBytes);
    if(QoS.value(mqttFixedHeader.qos)> 0) {
      buf.putShort(variableHeader.messageId.toShort);
    }
    buf.put(payload);
    return buf;
  }

  def encodeMessageWithOnlySingleByteFixedHeaderAndMessageId(message: Message):ByteBuffer =  {
    val mqttFixedHeader = message.getFixedHeader
    val variableHeader = message.getVariableHeader.asInstanceOf[MessageIdVariableHeader];
    val msgId = variableHeader.messageId();

    val variableHeaderBufferSize = 2; // variable part only has a message id
    val fixedHeaderBufferSize = 1 + getVariableLengthInt(variableHeaderBufferSize);
    val buf = ByteBuffer.allocate(fixedHeaderBufferSize + variableHeaderBufferSize);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte);
    writeVariableLengthInt(buf, variableHeaderBufferSize);
    buf.putShort(msgId.toShort);
    return buf;
  }

  def encodeMessageWithOnlySingleByteFixedHeader(message: Message) : ByteBuffer = {
    val  mqttFixedHeader = message.getFixedHeader;
    val buf = ByteBuffer.allocate(2);
    buf.put(getFixedHeaderByte1(mqttFixedHeader).toByte)
    buf.put(0.toByte);
    return buf;
  }

  def getFixedHeaderByte1(header:FixedHeader):Int = {
    var ret = 0;
    ret |= MessageType.value(header.messageType) << 4;
    if (header.isDup) {
      ret |= 0x08;
    }
    ret |= QoS.value(header.qos) << 1;
    if (header.isRetain) {
      ret |= 0x01;
    }
    return ret;
  }

  def writeVariableLengthInt(buffer: ByteBuffer, num:Int) =  {
    var numCopy = num
    do {
      var digit = num % 128;
      numCopy /= 128;
      if (numCopy > 0) {
        digit |= 0x80;
      }
      buffer.put(digit.toByte);
    } while (num > 0);
  }

  def getVariableLengthInt(num:Int):Int ={
    var count = 0;
    var numCopy = num
    do {
      numCopy  = numCopy/128;
      count += 1;
    } while (numCopy > 0);
    return count;
  }

  def encodeStringUtf8(s: String):Array[Byte] = {
    return s.getBytes("UTF-8")
  }
}
