package io

import java.net.InetSocketAddress

import akka.actor._
import akka.io.{Tcp, IO}
import broker.MessageBus
import handler.{SessionManager, Handler}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Server(sessions: ActorRef) extends Actor with ActorLogging{
  import akka.io.Tcp._
  import context.system
  IO(Tcp) ! Bind(self,new InetSocketAddress("localhost",1883))
  def receive = {
    case b @ Bound(address) => log.info("bound to "+ address)
    case CommandFailed(_) => log.info("failed to connect stopping server");context stop self
    case c @ Connected(remote, local) =>{
      log.info(s"connected remote $remote local $local connection $c")
      val handler = context.actorOf(Props(classOf[Handler],sessions))
      sender ! Register(handler)
    }
  }

}
object Server{
  def start(): Unit ={
    val system = ActorSystem("mqtt")
    val bus = MessageBus()
    val sessions = system.actorOf(Props(classOf[SessionManager],bus),"sessionManager")
    system.actorOf(Props(classOf[Server],sessions),"server")
  }
}
