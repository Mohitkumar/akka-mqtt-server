package broker

import akka.actor._
import akka.event.{LookupClassification, EventBus}
import broker.MessageBus.{ClassifierWithQos, Msg}

/**
  * Created by Mohit Kumar on 2/23/2017.
  */
class MessageBus extends EventBus with LookupClassification {
  override type Event = Msg
  override type Classifier = String
  override type Subscriber = ActorRef
  override protected def mapSize(): Int = 128
  private var subscriptions: List[(Classifier, Subscriber, Int)] =List()
  private val retains: List[(Classifier, Any)] = List()
  override protected def publish(event: Event, subscriber: Subscriber): Unit = {
    subscriber ! event.payload
  }

  override protected def classify(event: Event): Classifier = {
    event.topic
  }

  def subscribe(subscriber: Subscriber, to: Classifier, qos:Int): Boolean ={
    retains.filter(ret => ret._1 == to).foreach(t =>{
      subscriber ! ""
    })
    val similar = subscriptions.filter(sub => sub._1 == to && sub._2 == subscriber)
    if(similar.isEmpty){
      subscriptions = (to,subscriber,qos) :: subscriptions
      subscribers.put(to, subscriber)
    }else{
      subscriptions = (to,subscriber,qos) :: subscriptions.filter(sub => !(sub._1 == to && sub._2 == subscriber))
      similar.foreach(sim => subscribers.remove(sim._1,sim._2))
      subscribers.put(to, subscriber)
    }
  }
  override def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = {
    subscriptions = subscriptions.filter(sub => !(sub._1 == from && sub._2 == subscriber))
    super.unsubscribe(subscriber,from)
  }
  override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int = a.compareTo(b)
}

object MessageBus{
  case class Msg(topic: String, payload: Any)
  case class ClassifierWithQos(topic:String, qos:Int)
  def apply() = new MessageBus()

  def main(args: Array[String]) {
    val actorSystem = ActorSystem("testActorSystem")
    val actor1 = actorSystem.actorOf(Props[TestActor],"testActor1")
    val actor2 = actorSystem.actorOf(Props[TestActor],"testActor2")
    val actor3 = actorSystem.actorOf(Props[TestActor],"testActor3")
    val bus = MessageBus()
    bus.subscribe(actor1,"test")
    bus.subscribe(actor2,"test")
    bus.publish(Msg("test","datakkk"))
    bus.subscribe(actor3,"test")
    bus.publish(Msg("test","dsfdsf"))
  }
}
class TestActor extends Actor with ActorLogging{
  def receive = {
    case obj:Any =>log.info("received " +obj + " from " + sender)
    case _ => println("not ")
  }
}
