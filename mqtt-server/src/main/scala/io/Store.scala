package io

import akka.actor.{ActorLogging, Actor}
import akka.actor.Status.{Success, Failure}

import scala.collection.mutable.Map

/**
  * Created by Mohit Kumar on 2/22/2017.
  */
class Store extends Actor with ActorLogging{
  val dataStore: Map[String, Any] = Map[String, Any]()
  import io.Store._

  def receive = {
    case SetRequest(key, value) => {
      log.info(s"set request with key = $key, value = $value")
      dataStore  += ((key, value))
      sender ! Success
    }
    case GetRequest(key) => {
      log.info(s"get request with key = $key")
      val k = dataStore.get(key)
      k match {
        case Some(x) => sender ! x
        case None => sender ! Failure(new KeyNotFoundException(key))
      }
    }
    case o => sender ! Failure(new ClassNotFoundException())
  }
}
object Store{
  case class SetRequest(key: String, value: Any)
  case class GetRequest(key: String)
  case class KeyNotFoundException(key: String) extends Exception
}
