package handler

import akka.actor.{FSM, ActorLogging, Actor, ActorRef}
import akka.util.Timeout
import codec.MqttMessage._
import com.sun.xml.internal.ws.api.message.Packet
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
case class ConnectionSessionData(session:ActorRef, connection: ActorRef) extends ConnectionData
class ConnectionHandler(sessions: ActorRef) extends FSM[ConnectionState, ConnectionData] with ActorLogging {

  startWith(Waiting, EmptyConnectionData)

  when(Waiting){
    case Event(ReceivedPacket(msg: Message, CONNECT),_) =>{
      implicit val timeout = Timeout(1.seconds)
      val session =Await.result((sessions ? SessionReceived(msg, CONNECT)).mapTo[ActorRef],timeout.duration)
      session ! SessionReceived(msg, CONNECT)
      goto(Active) using ConnectionSessionData(session, sender)
    }
    case x => {
      log.info("unexpected " + x + " for waiting. Closing peer")
      sender ! Closing
      stay
    }
  }
  when(Active){
    case Event(p: Message, data: ConnectionSessionData) => {
      data.connection ! SendingPacket(p)
      stay
    }

    case Event(ReceivedPacket(msg: Message, CONNECT), data: ConnectionSessionData) => {
      log.info("Unexpected Connect. Closing peer")
      data.connection ! Closing
      stay
    }

    case Event(ReceivedPacket(msg:Message, DISCONNECT), data: ConnectionSessionData) => {
      log.info("Disconnect. Closing peer")
      data.connection ! Closing
      stay
    }

    case Event(ReceivedPacket(msg:Message,x), data: ConnectionSessionData) => {
      data.session ! SessionReceived(msg,x)
      stay
    }

    case Event(WrongState, b: ConnectionSessionData) => {
      b.connection ! Closing
      stay
    }

    case Event(KeepAliveTimeout, b: ConnectionSessionData) => {
      log.info("Keep alive timed out. Closing connection")
      b.connection ! Closing
      stay
    }
  }
  whenUnhandled {

    case Event(x, _) => {
      log.error("unexpected " + x + " for " + stateName + ". Closing peer")

      sender ! Closing

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
