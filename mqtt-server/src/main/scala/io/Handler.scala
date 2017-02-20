package io

import akka.actor.{ActorLogging, Actor}
import akka.io.Tcp
import akka.io.Tcp.{PeerClosed, Write, Received}
import codec.MqttMessage.CONNECT
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
        sender ! Write(data)
      }
      log.info(s"msg is $msg")
    }
    case PeerClosed => log.info("stopping handler ");context stop self
    case  d @ _ => log.info("could not understand"+d)
  }
}
