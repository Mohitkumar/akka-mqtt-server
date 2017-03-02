package handler

import java.nio.ByteBuffer

import akka.actor.{FSM, ActorRef}
import broker.MessageBus
import codec.MqttMessage._
import scala.concurrent.duration._

/**
  * Created by Mohit Kumar on 2/26/2017.
  */
case object ResetSession
case class PublishPayload(payload: Any, qos:Int)
case class CheckKeepAlive(period: FiniteDuration)

sealed trait SessionState
case object WaitingForNewSession extends SessionState
case object SessionConnected extends SessionState

sealed trait SessionData{
  val messageId: Int
  val cleanSession: Boolean
  val sending: List[Message]
}
case class SessionWaitingData(sending: List[Message] ,messageId: Int, cleanSession: Boolean) extends SessionData
case class SessionConnectedData(connection:ActorRef, cleanSession: Boolean, messageId:Int, sending: List[Message], will: Option[PublishMessage], last_packet: Long) extends SessionData
case object KeepAliveTimeout
case object WrongState
case object ConnectionLost

class Session(bus: MessageBus) extends FSM[SessionState,SessionData]{
  startWith(WaitingForNewSession,SessionWaitingData(List(),1,true))

  when(WaitingForNewSession){
    case Event(SessionReceived(msg:Message,CONNECT),data: SessionWaitingData) =>{
      val connectMessage = msg.asInstanceOf[ConnectMessage]
      val cleanSession = connectMessage.getVariableHeader.isCleanSession
      val clientId = connectMessage.payload.clientIdentifier
      var status = false
      if(!cleanSession && clientId.isEmpty){
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_REFUSED_IDENTIFIER_REJECTED,false))
      }else if(!cleanSession  && !data.cleanSession){
        status = true
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_ACCEPTED,true))
      }else{
        status = true
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_LEAST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_ACCEPTED,false))
      }
      if(status){
        if(!cleanSession){
          data.sending.foreach(f =>{
            if(f.getFixedHeader.messageType == PUBLISH){
              val fPublish = f.asInstanceOf[PublishMessage]
              val toBeSent = fPublish.copy(fixedHeader = fPublish.fixedHeader.copy(isDup = true))
              sender ! toBeSent
            }
            if(f.getFixedHeader.messageType == PUBREL){
              sender ! f
            }
          })
        }

        if(connectMessage.getVariableHeader.keepAliveTimeSeconds > 0){
          val delay = connectMessage.getVariableHeader.keepAliveTimeSeconds.seconds
          context.system.scheduler.scheduleOnce(delay, self,CheckKeepAlive(delay))(context.system.dispatcher)
        }

        val will = if(connectMessage.getVariableHeader.isWillFlag){
          val willMsg = connectMessage.getPayload.willMessage
          val data = ByteBuffer.allocate(willMsg.length)
          willMsg.foreach(f => data.putChar(f))
          Some(PublishMessage(FixedHeader(PUBLISH,false,QoS.getQos(connectMessage.getVariableHeader.willQos),connectMessage.getVariableHeader.isWillRetain,0),
            PublishVariableHeader(connectMessage.payload.willTopic,0),data))
        } else None

        goto(SessionConnected) using SessionConnectedData(sender,cleanSession,data.messageId,List(),will, System.currentTimeMillis())
      }else{
        stay
      }
    }

    case Event(SessionReceived(_,_),_) =>{
      sender ! WrongState
      stay
    }
  }

  when(SessionConnected) {
    case Event(CheckKeepAlive(delay), data: SessionConnectedData) => {
      if ((System.currentTimeMillis() - delay.toMillis) < data.last_packet) {
        log.info("checking keepalive OK")
        context.system.scheduler.scheduleOnce(delay, self, CheckKeepAlive(delay))(context.system.dispatcher)
        stay
      } else {
        log.info("checking keepalive is NOT ok")
        data.connection ! KeepAliveTimeout
        self ! ConnectionLost
        stay
      }
    }

    case Event(SessionReceived(msg:Message, SUBSCRIBE), data:SessionConnectedData) =>{
      val subMsg = msg.asInstanceOf[SubscriptionMessage]
      subMsg.getPayload.topicSubscriptions.foreach(t =>{
        bus.subscribe(self,t.topicFilter)
      })
      stay
    }
  }

  onTransition(handler _)

  def handler(from: SessionState, to: SessionState): Unit = {

    log.info("State changed from " + from + " to " + to)
  }

  initialize()

}
