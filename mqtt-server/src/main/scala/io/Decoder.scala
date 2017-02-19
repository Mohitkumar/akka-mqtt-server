package io

import java.nio.ByteBuffer

import akka.util.ByteString
import codec.MqttMessage.{CONNECT, MessageType, FixedHeader}
import codec.MqttMessage._
import codec.{MQTT_3_1_1, MqttVersion}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Decoder {
  def decode(byteString: ByteString): Unit ={

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
}
