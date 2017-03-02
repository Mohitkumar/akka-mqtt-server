package broker

import akka.actor._
import akka.event.{LookupClassification, EventBus}
import broker.MessageBus.{Msg}
import handler.PublishPayload

/**
  * Created by Mohit Kumar on 2/23/2017.
  */
class MessageBus  {
  type Event = Msg
  type Classifier = String
  type Subscriber = ActorRef
  private var subscriptions: List[(Classifier, Subscriber, Int)] =List()
  private var retains: List[(Classifier, Any)] = List()
  def publish(event: Event): Unit = {
    subscriptions.filter(sub => sub._1 == event.topic).foreach(sub =>{
      sub._2 ! PublishPayload(event.payload,sub._3)
    })
    if(event.retain){
      if(event.clean_retain){
        retains = retains.filter(ret => ret._1 != event.topic)
      }else{
        retains = (event.topic,event.payload) :: retains
      }
    }
  }


  def subscribe(subscriber: Subscriber, to: Classifier, qos:Int) ={
    retains.filter(ret => ret._1 == to).foreach(t =>{
      subscriber ! PublishPayload(t._2,qos)
    })
    val similar = subscriptions.filter(sub => sub._1 == to && sub._2 == subscriber)
    if(similar.isEmpty){
      subscriptions = (to,subscriber,qos) :: subscriptions
    }else{
      subscriptions = (to,subscriber,qos) :: subscriptions.filter(sub => !(sub._1 == to && sub._2 == subscriber))
    }
  }
  def unsubscribe(subscriber: Subscriber, from: Classifier) = {
    subscriptions = subscriptions.filter(sub => !(sub._1 == from && sub._2 == subscriber))
  }

}

object MessageBus{
  case class Msg(topic: String, payload: Any, retain: Boolean = false, clean_retain: Boolean = false)
  def apply() = new MessageBus()

  def main(args: Array[String]) {
    val actorSystem = ActorSystem("testActorSystem")
    val actor1 = actorSystem.actorOf(Props[TestActor],"testActor1")
    val actor2 = actorSystem.actorOf(Props[TestActor],"testActor2")
    val actor3 = actorSystem.actorOf(Props[TestActor],"testActor3")
    val bus = MessageBus()
    bus.subscribe(actor1,"test",1)
    bus.subscribe(actor2,"test",1)
    bus.publish(Msg("test","datakkk",true,false))
    bus.subscribe(actor3,"test",1)
  }
}
class TestActor extends Actor with ActorLogging{
  def receive = {
    case obj:Any =>log.info("received " +obj + " from " + sender)
    case _ => println("not ")
  }
}
