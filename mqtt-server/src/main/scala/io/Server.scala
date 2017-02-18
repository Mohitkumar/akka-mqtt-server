package io

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props, ActorLogging, Actor}
import akka.io.{Tcp, IO}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Server extends Actor with ActorLogging{
  import akka.io.Tcp._
  import context.system
  IO(Tcp) ! Bind(self,new InetSocketAddress("localhost",8081))
  def receive = {
    case b @ Bound(address) => log.info("bound to "+ address)
    case CommandFailed(_) => log.info("failed to connect stopping server");context stop self
    case c @ Connected(remote, local) =>{
      log.info(s"connected remote $remote local $local connection $c")
      val handler = context.actorOf(Props[Handler])
      sender ! Register(handler)
    }
  }

}
object Server{
  def start(): Unit ={
    val system = ActorSystem("server-sys")
    val actor = system.actorOf(Props[Server],"server")
  }
}
