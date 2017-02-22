package io

import akka.actor.Status.Success
import akka.actor.{Props, ActorSystem}
import akka.util.Timeout
import io.Store.{GetRequest, SetRequest}

import scala.concurrent.{Await, Future}

/**
  * Created by Mohit Kumar on 2/22/2017.
  */
class Channel(timeout:Int = 1) {
  import scala.concurrent.duration._
  import akka.pattern.ask
  implicit val t = Timeout(timeout.seconds)
  implicit val system = ActorSystem("ChannelSys")

  val remoteDb = system.actorOf(Props[Store],"channel")

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
}
object Channel{
  def apply(timeOut:Int) = new Channel(timeOut)
  def apply() = new Channel()
}
