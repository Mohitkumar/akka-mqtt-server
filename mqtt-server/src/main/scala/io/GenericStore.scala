package io

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import io.Store.{Contains, Close, GetRequest, SetRequest}

import scala.concurrent.Await

/**
  * Created by Mohit Kumar on 2/25/2017.
  */
class GenericStore(actorName: String,timeout:Int = 1,system: ActorSystem = ActorSystem("genericStore")) {
  import scala.concurrent.duration._
  import akka.pattern.ask
  implicit val t = Timeout(timeout.seconds)

  val remoteDb = system.actorOf(Props[Store],actorName)

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
  def getAsActorRef(key: String):ActorRef = {
    val data= remoteDb ? GetRequest(key)
    Await.result(data.mapTo[ActorRef],2.seconds)
  }
  def contains(key: String):Boolean = {
    val res = Await.result((remoteDb ? Contains(key)).mapTo[Boolean],2.seconds)
    res
  }
  def getState: Unit ={
    remoteDb ! "state"
  }
  def close() = remoteDb ! Close
}
object GenericStore{
  def apply(actorName:String) = new GenericStore(actorName)
  def apply(actorName:String,system: ActorSystem) = new GenericStore(actorName,1,system)
  def apply(actorName:String,timeout:Int, system: ActorSystem) = new GenericStore(actorName,timeout,system)
}
