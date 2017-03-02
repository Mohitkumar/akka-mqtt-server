package handler

import akka.actor.{FSM, ActorLogging, Actor, ActorRef}
import akka.io.Tcp.{PeerClosed, Close, Received}
import akka.util.Timeout
import codec.MqttMessage._
import com.sun.xml.internal.ws.api.message.Packet
import io.{Encoder, Decoder}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern._

/**
  * Created by Mohit Kumar on 2/26/2017.
  */
case class SessionReceived(packet: Message,messageType:MessageType)
sealed trait ConnectionState
case object Active extends ConnectionState
case object Waiting extends ConnectionState
sealed trait ConnectionData
case object EmptyConnectionData extends ConnectionData
case class ConnectionSessionData(session:ActorRef, connection:ActorRef) extends ConnectionData
class ConnectionHandler(sessions: ActorRef) extends FSM[ConnectionState, ConnectionData] with ActorLogging {

  startWith(Waiting, EmptyConnectionData)

  when(Waiting){
    case Event(Received(data),_) =>{
      implicit val timeout = Timeout(2.seconds)
      val msg = Decoder.decodeMsg(data)
      if(msg.getFixedHeader.messageType == CONNECT){
        val session =Await.result((sessions ? SessionReceived(msg, CONNECT)).mapTo[ActorRef],timeout.duration)
        session ! SessionReceived(msg, CONNECT)
        goto(Active) using ConnectionSessionData(session, sender)
      }
      else {
        log.info("unexpected " + data + " for waiting. Closing peer")
        sender ! Close
        context stop  self
        stay
      }
    }
    case Event(PeerClosed,_) =>{
      log.info("stopping handler ")
      sender ! Close
      context stop  self
      stay
    }
  }
  when(Active){
    case Event(p:ConnAckMessage, data: ConnectionSessionData) => {
      val response = Encoder.encode(p)
      log.info(s"sending msg $p response $response to ${data.connection}")
      data.connection ! response
      stay
    }
    case Event(p:Message, data: ConnectionSessionData) => {
      val response = Encoder.encode(p)
      log.info(s"sending msg $p response $response to ${data.connection}")
      data.connection ! response
      stay
    }
    case Event(Received(datarec), data: ConnectionSessionData) => {
      val msg = Decoder.decodeMsg(datarec)
      if(msg.getFixedHeader.messageType == CONNECT){
        log.info("Unexpected Connect. Closing peer")
        sender ! Close
        context stop  self
      }else if(msg.getFixedHeader.messageType == DISCONNECT){
        log.info("Disconnect. Closing peer")
        sender ! Close
        context stop  self
      }else{
        data.session ! SessionReceived(msg,msg.getFixedHeader.messageType)
      }
      stay
    }

    case Event(WrongState, b: ConnectionSessionData) => {
      log.info("in wrong state")
      b.connection ! Close
      context stop  self
      stay
    }

    case Event(KeepAliveTimeout, b: ConnectionSessionData) => {
      log.info("Keep alive timed out. Closing connection")
      b.connection ! Close
      context stop  self
      stay
    }
    case Event(PeerClosed,_) =>{
      log.info("stopping handler ")
      context stop self
      stay
    }
  }
  whenUnhandled {

    case Event(x, _) => {
      log.error("unexpected " + x + " for " + stateName + ". Closing peer")
      sender ! Close
      context stop  self
      stay
    }
  }

  onTermination {
    case StopEvent(x, s, d) => {
      log.info("Terminated with " + x + " and " + s + " and " + d)
    }
  }

  onTransition(handler _)

  def handler(from: ConnectionState, to: ConnectionState): Unit = {
    log.info("State changed from " + from + " to " + to)
  }

  initialize()
}
