package io

import akka.actor.{ActorLogging, Actor}
import akka.io.Tcp
import akka.io.Tcp.{PeerClosed, Write, Received}

/**
  * Created by Mohit Kumar on 2/18/2017.
  */
class Handler extends Actor with ActorLogging{
  import Tcp._
  def receive = {
    case Received(data) => log.info("received data" + data);sender ! Write(data)
    case PeerClosed => log.info("stopping handler ");context stop self
    case  d @ _ => log.info("could not understand"+d)
  }
}
