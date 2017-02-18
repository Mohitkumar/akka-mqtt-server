package io

import akka.util.ByteString
import codec.MqttMessage.{MessageType, FixedHeader}

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
  }
}
