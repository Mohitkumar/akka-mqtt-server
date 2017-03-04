package handler

import akka.actor.{ActorLogging, ActorRef, FSM}
import akka.io.Tcp.{Close, PeerClosed, Received, Write}
import akka.pattern._
import akka.util.Timeout
import codec.{Encoder, Decoder}
import codec.MqttMessage._

import scala.concurrent.Await
import scala.concurrent.duration._

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
      log.info(s"received request ${msg.getFixedHeader.messageType}")
      if(msg.getFixedHeader.messageType == CONNECT){
        val session =Await.result((sessions ? SessionReceived(msg, CONNECT)).mapTo[ActorRef],timeout.duration)
        session ! SessionReceived(msg, CONNECT)
        goto(Active) using ConnectionSessionData(session, sender)
      }
      else {
        log.info(s"unexpected $data while waiting. Closing peer")
        sender ! Close
        context stop  self
        stay
      }
    }
    case Event(PeerClosed,_) =>{
      log.info("stopping handler PeerClosed received from client")
      sender ! Close
      context stop  self
      stay
    }
  }
  when(Active){
    case Event(p:ConnAckMessage, data: ConnectionSessionData) => {
      val response = Encoder.encode(p)
      log.info(s"sending CONNACK to client")
      data.connection ! Write(response)
      stay
    }
    case Event(p:PubAckMessage, data: ConnectionSessionData) => {
      val response = Encoder.encode(p)
      log.info(s"sending PUBACK to client")
      data.connection ! Write(response)
      stay
    }
    case Event(p:Message, data: ConnectionSessionData) => {
      val response = Encoder.encode(p)
      log.info(s"sending ${p.getFixedHeader.messageType} to client")
      data.connection ! Write(response)
      stay
    }
    case Event(Received(datarec), data: ConnectionSessionData) => {
      val msg = Decoder.decodeMsg(datarec)
      log.info(s"received ${msg.getFixedHeader.messageType} from client")
      if(msg.getFixedHeader.messageType == CONNECT){
        log.info("Unexpected Connect. Closing peer")
        sender ! Close
        context stop  self
      }else if(msg.getFixedHeader.messageType == DISCONNECT){
        log.info("Disconnect recieved from client Closing peer")
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
      log.info("PeerClosed received stopping handler ")
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
      log.debug("Terminated with " + x + " and " + s + " and " + d)
    }
  }

  onTransition(handler _)

  def handler(from: ConnectionState, to: ConnectionState): Unit = {
    log.debug("State changed from " + from + " to " + to)
  }

  initialize()
}
