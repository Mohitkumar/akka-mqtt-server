package handler

import java.net.URLEncoder
import java.util.UUID

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.event.EventBus
import broker.MessageBus
import codec.MqttMessage.{ConnectMessage, CONNECT}

/**
  * Created by Mohit Kumar on 2/26/2017.
  */
class SessionManager(bus: MessageBus) extends Actor with ActorLogging{
  def getActorName(clientId: String):String = {
    val cid = if(clientId.isEmpty) UUID.randomUUID().toString else clientId
    URLEncoder.encode(cid,"UTF-8")
  }

  def getOrCreateSession(actorName: String, cleanSession:Boolean):ActorRef = {
    val c = context.child(actorName)
    if(c.isDefined && !cleanSession) c.get
    if(c.isDefined && cleanSession){
      c.get ! ResetSession
      return c.get
    }
    val a = context.actorOf(Props(classOf[Session],bus), actorName)
    context watch a
    a
  }
  def receive = {
    case SessionReceived(msg, CONNECT) =>{
      val connectMessage = msg.asInstanceOf[ConnectMessage]
      val name = getActorName(connectMessage.payload.clientIdentifier)
      val session = getOrCreateSession(name,connectMessage.variableHeader.isCleanSession)
      sender ! session
    }
    case x  => log.info(s"unexpected message $x")
  }
}
