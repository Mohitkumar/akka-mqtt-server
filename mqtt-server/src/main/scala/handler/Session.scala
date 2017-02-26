package handler

import akka.actor.ActorRef

/**
  * Created by Mohit Kumar on 2/26/2017.
  */
case object ResetSession
sealed trait SessionState

case object WaitingForNewSession extends SessionState
case object SessionConnected extends SessionState

case object KeepAliveTimeout
case object WrongState
class Session(bus: ActorRef) {

}
