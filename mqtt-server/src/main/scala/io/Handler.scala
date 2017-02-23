package io

import java.nio.ByteBuffer

import akka.actor.{Props, ActorLogging, Actor}
import akka.io.Tcp
import akka.io.Tcp.{PeerClosed, Write, Received}
import codec.MqttMessage._
import handler.ConnectHandler
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
      log.info(s"decoded msg is $msg")
      val channel = Channel(context.system)
      val fixedHeader = msg.getFixedHeader
      if(fixedHeader.messageType == CONNECT){
        val connectHandler = context.actorOf(Props(classOf[ConnectHandler],channel),"connectHandler")
        connectHandler forward msg
      }
      /*if(fixedHeader.messageType == PUBLISH){
        val payload = msg.getPayload.asInstanceOf[ByteBuffer]
        println(new String(payload.array(),"UTF-8"))
        val fixedHeader = FixedHeader(PUBACK,false,EXACTLY_ONCE,false,0)
        val variableHeader = MessageIdVariableHeader(1)
        val pubAckMessage = PubAckMessage(fixedHeader,variableHeader)
        sender() ! Write(Encoder.encode(pubAckMessage))
      }
      if(fixedHeader.messageType == SUBSCRIBE){
        val fixedHeader = FixedHeader(SUBACK,false,AT_LEAST_ONCE,false,0)
        val variableHeader = MessageIdVariableHeader(1)
        val payload = SubAckPayload(List(0))
        val subAckMsg = SubAckMessage(fixedHeader,variableHeader,payload)
        sender ! Write(Encoder.encode(subAckMsg))
      }*/
    }
    case PeerClosed => log.info("stopping handler ");context stop self
    case  d @ _ => log.info("could not understand"+d)
  }
}
