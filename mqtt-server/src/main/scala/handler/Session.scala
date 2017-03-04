package handler


import akka.actor.{FSM, ActorRef}
import broker.MessageBus
import broker.MessageBus.Msg
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
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_MOST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_REFUSED_IDENTIFIER_REJECTED,false))
      }else if(!cleanSession  && !data.cleanSession){
        status = true
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_MOST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_ACCEPTED,true))
      }else{
        status = true
        sender ! ConnAckMessage(FixedHeader(CONNACK,false,AT_MOST_ONCE,false,0),ConnAckVariableHeader(CONNECTION_ACCEPTED,false))
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
          Some(PublishMessage(FixedHeader(PUBLISH,false,QoS.getQos(connectMessage.getVariableHeader.willQos),connectMessage.getVariableHeader.isWillRetain,0),
            PublishVariableHeader(connectMessage.payload.willTopic,0),willMsg))
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
      val topics = subMsg.getPayload.topicSubscriptions;
      topics.foreach(t =>{
        bus.subscribe(self,t.topicFilter,QoS.value(subMsg.getFixedHeader.qos))
      })
      stay using data.copy(last_packet = System.currentTimeMillis())
    }

    case Event(SessionReceived(msg:Message, PUBLISH),data: SessionConnectedData) =>{
      val pubMsg = msg.asInstanceOf[PublishMessage]
      if(msg.getFixedHeader.qos == AT_MOST_ONCE){
        sender ! PubAckMessage(FixedHeader(PUBACK,false,AT_MOST_ONCE,false,0),MessageIdVariableHeader(pubMsg.getVariableHeader.messageId))
      }
      if(msg.getFixedHeader.qos == EXACTLY_ONCE){
        sender ! new Message(FixedHeader(PUBREC,false,AT_MOST_ONCE,false,0),MessageIdVariableHeader(pubMsg.getVariableHeader.messageId),null)
      }
      if(!pubMsg.getFixedHeader.isRetain && (pubMsg.payload == null || pubMsg.payload.length == 0)){
        log.warning("got message with retain false and empty payload")
      }
      bus.publish(Msg(pubMsg.getVariableHeader.topicName,pubMsg
        ,pubMsg.getFixedHeader.isRetain, pubMsg.getFixedHeader.isRetain && pubMsg.getPayload.length == 0))

      stay using data.copy(last_packet = System.currentTimeMillis())
    }

    case Event(SessionReceived(msg:Message, PUBREC), data: SessionConnectedData) =>{
      val pubrel = new Message(FixedHeader(PUBREL,false,AT_MOST_ONCE,false,0),MessageIdVariableHeader(msg.getVariableHeader.asInstanceOf[MessageIdVariableHeader].messageId))
      sender ! pubrel
      stay using data.copy(last_packet = System.currentTimeMillis(), sending = data.sending.filter(f =>{
        if(f.getFixedHeader.messageType == PUBLISH
          && f.getVariableHeader.asInstanceOf[PublishVariableHeader].messageId == msg.getVariableHeader.asInstanceOf[MessageIdVariableHeader].messageId){
           true
        }else{
           false
        }
      }) :+ pubrel)
    }

    case Event(SessionReceived(msg:Message, PUBREL),data:SessionConnectedData) =>{
      sender ! new Message(FixedHeader(PUBCOMP,false,AT_MOST_ONCE,false,0),MessageIdVariableHeader(msg.getVariableHeader.asInstanceOf[MessageIdVariableHeader].messageId))
      stay using data.copy(last_packet = System.currentTimeMillis())
    }
    case Event(SessionReceived(msg:Message, PUBACK),data:SessionConnectedData) =>{
      stay using data.copy(last_packet = System.currentTimeMillis(),sending = data.sending.filter(f =>{
        if(f.getFixedHeader.messageType == PUBLISH
          && f.getVariableHeader.asInstanceOf[PublishVariableHeader].messageId == msg.getVariableHeader.asInstanceOf[MessageIdVariableHeader].messageId){
          true
        }else{
          false
        }
      }))
    }

    case Event(SessionReceived(msg:Message, PUBCOMP), data:SessionConnectedData)=>{
      stay using data.copy(last_packet = System.currentTimeMillis(),sending = data.sending.filter(f =>{
        if(f.getFixedHeader.messageType == PUBREL
          && f.getVariableHeader.asInstanceOf[PublishVariableHeader].messageId == msg.getVariableHeader.asInstanceOf[MessageIdVariableHeader].messageId){
          true
        }else{
          false
        }
      }))
    }

    case Event(SessionReceived(msg:Message,UNSUBSCRIBE),data:SessionConnectedData)=>{
      val unsubMsg = msg.asInstanceOf[UnsubscribeMessage]
      unsubMsg.getPayload.topics.foreach(t => bus.unsubscribe(self,t))
      sender ! UnsubAckMessage(FixedHeader(UNSUBACK,false,AT_MOST_ONCE,false,0),MessageIdVariableHeader(unsubMsg.getVariableHeader.messageId))
      stay using data.copy(last_packet = System.currentTimeMillis())
    }

    case Event(SessionReceived(msg:Message, PINGREQ), data:SessionConnectedData) =>{
      sender ! new Message(FixedHeader(PINGRESP,false,AT_MOST_ONCE,false,0))
      stay using data.copy(last_packet = System.currentTimeMillis())
    }

    case Event(ConnectionLost,data:SessionConnectedData) =>{
      if(data.will.isDefined){
        val resp = data.will.get.copy(variableHeader = data.will.get.getVariableHeader.copy(messageId = data.messageId))
        bus.publish(Msg(resp.getVariableHeader.topicName,resp.getPayload,resp.getFixedHeader.isRetain,resp.getFixedHeader.isRetain && resp.payload.length == 0))

      }
      goto(WaitingForNewSession) using SessionWaitingData(data.sending,1,data.cleanSession)
    }

    case Event(x@PublishPayload(p:PublishMessage,_),data:SessionConnectedData) =>{
        val qos = x.qos min QoS.value(p.getFixedHeader.qos)
        val pub = PublishMessage(FixedHeader(PUBLISH,false,QoS.getQos(qos),false,0)
          ,PublishVariableHeader(p.variableHeader.topicName,if(qos == 0) 0 else p.getVariableHeader.messageId),p.payload)
        data.connection ! pub
        if(qos >0){
          stay using data.copy(messageId = data.messageId +1, sending = data.sending :+ pub)
        }else{
          stay
        }
    }
  }

  whenUnhandled {
    case Event(x, data) => {
      log.error("unexpected message " + x + " for " + stateName)

      goto(WaitingForNewSession) using SessionWaitingData(data.sending , 1, data.cleanSession)
    }
  }
  onTransition(handler _)

  def handler(from: SessionState, to: SessionState): Unit = {

    log.info("State changed from " + from + " to " + to)
  }

  initialize()

}
