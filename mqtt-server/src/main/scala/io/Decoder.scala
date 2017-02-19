package io

import akka.util.ByteString
import codec.MqttMessage.{CONNECT, MessageType, FixedHeader}
import codec.MqttMessage._

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Decoder {
  def decode(byteString: ByteString): Unit ={

  }

  def decodeFixedHeader(byteString: ByteString):FixedHeader = {
    val buffer = byteString.asByteBuffer
    val b1 = buffer.getShort
    val msgType = MessageType.getMessageType(b1 >> 4)
    val dupFlag = b1.&(0x08) == 0x08
    val qosLevel = (b1 & 0x06) >> 1
    val retain = (b1 & 0x01) != 0
    var remainingLength = 0;
    var multiplier = 1;
    var digit: Short = 0
    var loops = 0;
    do {
      digit = buffer.getShort();
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
}
