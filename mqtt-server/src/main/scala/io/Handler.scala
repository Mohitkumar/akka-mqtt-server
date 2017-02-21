package io

import java.nio.ByteBuffer

import akka.actor.{ActorLogging, Actor}
import akka.io.Tcp
import akka.io.Tcp.{PeerClosed, Write, Received}
import codec.MqttMessage._
import io.Decoder

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Handler extends Actor with ActorLogging{
  import Tcp._
  def receive = {
    case Received(data) => {
      log.info("received data" + data)
      val msg = Decoder.decodeMsg(data)
      val fixedheader = msg.getFixedHeader
      if(fixedheader.messageType == CONNECT){
        val fixedHeader = FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,0)
        val variableHeader  = ConnAckVariableHeader(CONNECTION_ACCEPTED,false);
        val connAckMessage = ConnAckMessage(fixedHeader,variableHeader)
        val response = Encoder.encode(connAckMessage)
        log.info(s"writing data back $response")
        sender() ! Write(response)
      }
      if(fixedheader.messageType == PUBLISH){
        val payload = msg.getPayload.asInstanceOf[ByteBuffer]
        println(new String(payload.array(),"UTF-8"))
        val fixedHeader = FixedHeader(PUBACK,false,EXACTLY_ONCE,false,0)
        val variableHeader = MessageIdVariableHeader(1)
        val pubAckMessage = PubAckMessage(fixedHeader,variableHeader)
        sender() ! Write(Encoder.encode(pubAckMessage))
      }
      log.info(s"msg is $msg")
    }
    case PeerClosed => log.info("stopping handler ");context stop self
    case  d @ _ => log.info("could not understand"+d)
  }
}
