package broker

import akka.actor.ActorRef
import akka.event.{LookupClassification, EventBus}
import broker.MessageBus.Msg

/**
  * Created by Mohit Kumar on 2/23/2017.
  */
class MessageBus extends EventBus with LookupClassification{
  override type Event = Msg
  override type Classifier = String
  override type Subscriber = ActorRef
  override protected def mapSize(): Int = 128

  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }

  override protected def classify(event: Event): Classifier = {
    event.topic
  }

  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = a.compareTo(b)
}

object MessageBus{
  case class Msg(topic:String, payload:Any)
  def apply() = new MessageBus()
}
