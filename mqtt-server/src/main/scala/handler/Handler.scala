package handler

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.io.Tcp
import codec.MqttMessage._
import io.{Decoder, Encoder}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
case class ReceivedPacket(packet: Message,messageType:MessageType)
case class SendingPacket(packet: Message)
case object Closing

class Handler(sessions: ActorRef) extends Actor with ActorLogging{
  import Tcp._
  def receive = {
    case Received(data) => {
      log.info("received data" + data)
      val msg = Decoder.decodeMsg(data)
      log.info(s"decoded msg is $msg")
      val connection = context.actorOf(Props(classOf[ConnectionHandler],sessions))
      connection ! msg
    }
    case SendingPacket(msg) => sender ! Encoder.encode(msg)
    case PeerClosed => log.info("stopping handler ");context stop self
    case Closing  => log.info("stopping handler forcibly"); sender ! Close;context stop  self
    case  d @ _ => log.info("could not understand"+d);context stop self
  }
}
