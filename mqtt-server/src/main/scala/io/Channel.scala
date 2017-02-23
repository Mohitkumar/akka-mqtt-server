package io

import akka.actor.Status.Success
import akka.actor.{Props, ActorSystem}
import akka.io.Tcp.Close
import akka.util.Timeout
import io.Store.{GetRequest, SetRequest}

import scala.concurrent.{Await, Future}

/**
  * Created by Mohit Kumar on 2/22/2017.
  */
class Channel(timeout:Int = 1, implicit val system:ActorSystem = ActorSystem("ChannelSys")) {
  import scala.concurrent.duration._
  import akka.pattern.ask
  implicit val t = Timeout(timeout.seconds)
  //implicit val system = ActorSystem("ChannelSys")

  val remoteDb = system.actorOf(Props[Store])

  def put(key:String, value:Any) ={
    remoteDb ? SetRequest(key,value)
  }

  def get(key: String):Any = {
    val data= remoteDb ? GetRequest(key)
    Await.result(data.mapTo[Any],2.seconds)
  }
  def getAsString(key: String):String = {
    val data= remoteDb ? GetRequest(key)
    Await.result(data.mapTo[String],2.seconds)
  }
  def getAsInt(key: String):Int = {
    val data= remoteDb ? GetRequest(key)
    Await.result(data.mapTo[Int],2.seconds)
  }

  def close() = remoteDb ! Close
}
object Channel{
  def apply(timeOut:Int) = new Channel(timeOut)
  def apply(system: ActorSystem) = new Channel(1,system)
  def apply() = new Channel()
}
