package nats

import akka.actor.{Actor, ActorSystem, Props}

import java.util.Properties
import org.nats._

class Subscriber extends Actor {
  val conn = {
    val opts = new Properties
    //opts.put("servers", "nats://localhost:4242")
    Conn.connect(opts, null)
  }
  var sid = -1
  def receive = {
    // subscribe
    case "start" => sid =conn.subscribe("channel", (msg: Msg) => {
      println("Received update : " + msg.body)
    })
    case "stop" => conn.unsubscribe(sid); conn.close 
  }
}

object sample1 {

  def main(args: Array[String]) {
    //val system = ActorSystem("nats-sample")
    //val subscriber = system.actorOf(Props(new Subscriber), "subscriber")
    //subscriber ! "start"

    val opts = new Properties
    //opts.put("servers", "nats://localhost:4242")
    val conn = Conn.connect(opts)
println("connected")
    // Simple Publisher
    conn.publish("channel", "world")
println("published")

    // Simple Subscriber
    val sid = conn.subscribe("channel", (msg:Msg) => {println("Received update : " + msg.body)})
println("")
    // Unsubscribing
    conn.unsubscribe(sid);

    // Requests
    conn.request("help", (msg:Msg) => {
      println("Got a response for help : " + msg.body)
      sys.exit
    })      

    // Replies
    conn.subscribe("help", (msg:Msg) => {conn.publish(msg.reply, "I can help!")})

    //Thread.sleep(10000)
    conn.close();
    //subscriber ! "stop"
    println("finished")
  }
}
