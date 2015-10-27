package nats.cf

import nats.client.{Message, MessageHandler, Nats, NatsConnector}
import java.util.concurrent.TimeUnit

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.Formats._

import org.joda.time.DateTime
//import scala.tools.nsc.io.File
import java.io.{File, FileWriter}
import com.amazonaws.auth.PropertiesFileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectResult

import nl.grons.metrics.scala._
import akka.actor.{Actor, ActorSystem, Props}

import scala.concurrent._
import scala.concurrent.duration._
//import ExecutionContext.Implicits.global
import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor}

object Global {
  /** The application wide metrics registry. */
  val metricRegistry = new com.codahale.metrics.MetricRegistry()
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Global.metricRegistry
}

class AuctionHandler(val ct: Timer) extends MessageHandler {
  val s3 = new AmazonS3Client(new PropertiesFileCredentialsProvider("/Users/bachir/.aws/credentials"))
  val uuid: String = java.util.UUID.randomUUID.toString
  implicit val formats = DefaultFormats
  var writerByName = scala.collection.mutable.Map[String, FileWriter]()
  var fileByName = scala.collection.mutable.Map[String, File]()

  override def onMessage(message: Message) {
    ct.time {
      val as = parse(message.getBody)
      val id = (as \ "id").extract[String]
      val timestamp = (as \ "timestamp").extract[String]
      val publisherId = (as \ "site" \ "publisher" \ "id").extract[Int]
      val date: DateTime = new DateTime(timestamp)
      val hour = date.hourOfDay.get
      val day = date.dayOfMonth.get
      val month = date.monthOfYear.get
      val year = date.year.get
      val filename = s"$year-$month-$day-$hour-$uuid.json"
      val writerOp = writerByName.get(filename)
      val writer: FileWriter = writerOp match {
        case Some(w) => w
        case None => {
          val w = new FileWriter(filename)
          writerByName += (filename -> w)
          w
        }
      }
      writer.append(message.getBody)
      val file: File = fileByName.get(filename) match {
        case Some(f) => f
        case None => {
          val f = new File(filename)
          fileByName += (filename -> f)
          f
        }
      }
      if(file.length > 1000000) {
        writer.flush() //writer.close //writer.flush()
        val result: PutObjectResult = s3.putObject("adomik-firehose-dump", s"$year/$month/$day/$hour/$uuid.json", file)
        println("Pushed file to S3: "+result.getMetadata.toString)
        //writer.close
        //file.delete; fileByName -= filename; writerByName -= filename;
        //fileByName(filename) = new File(filename) 
        //writerByName(filename) = new FileWriter(filename)
      }
    }
  }
}

object sample2 extends Instrumented {
  val pt: Timer = metrics.timer("produce")
  val ct: Timer = metrics.timer("consume")

  implicit val ec = new ExecutionContext {
    val threadPool = new ThreadPoolExecutor(
      5, // core thread pool size
      8, // maximum thread pool size
      3, // time to wait before resizing pool
      TimeUnit.MINUTES, 
      new ArrayBlockingQueue[Runnable](7, true))


    def execute(runnable: Runnable) {
        threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable) {}
  }

  def main(args: Array[String]) {
 
    val nats: Nats = new NatsConnector().addHost("nats://localhost:7222").addHost("nats://localhost:8222").addHost("nats://localhost:9222").connect()
    val nats1: Nats = new NatsConnector().addHost("nats://localhost:7222").addHost("nats://localhost:8222").addHost("nats://localhost:9222").connect()
    val nats2: Nats = new NatsConnector().addHost("nats://localhost:7222").addHost("nats://localhost:8222").addHost("nats://localhost:9222").connect()
    val nats3: Nats = new NatsConnector().addHost("nats://localhost:7222").addHost("nats://localhost:8222").addHost("nats://localhost:9222").connect()
    // consume
    val sub1 = future {
      nats1.subscribe("auction_summary", "job.workers").addMessageHandler(new AuctionHandler(ct))
    }
    val sub2 = future {
      nats2.subscribe("auction_summary", "job.workers", new AuctionHandler(ct))
    }
    future {
      nats3.subscribe("auction_summary", "job.workers", new AuctionHandler(ct))
    }
    // produce
    val pub = future {
      (1 to 1000).foreach{i =>
        (1 to 1000).foreach { j =>
          pt.time {
          val uuid = java.util.UUID.randomUUID.toString
          val body = s""" {"id":"${uuid}","at":2,"timestamp":"2015-07-30T18:14:17.000Z","site":{"name":"2977-vast2-mpc.com","domain":"2977-vast2-mpc.com","cat":["IAB1-3"],"publisher":{"id":362042,"name":"2977 vast 2 MPC"}}, "device":{"ip":"10.172.161.34","ua":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/43.0.2357.130 Chrome/43.0.2357.130 Safari/537.36","language":"en","os":"Linux","devicetype":2},"user":{"id":"8984073634161885980"},"bid_responses":[{"timestamp":"2015-07-30T18:14:17.000Z","bidfloor":1.43,"buyer_name":"Legacy RTB Buyer","bidder_id":431521,"bidder_name":"smoker_prdm3690","currency":"USD","status":1,"seatbid":[{"bid":[{"status":1,"price":7.80}]}]}]} """
          nats.publish("auction_summary", body)
          }
        }
      }
    }
    Thread.sleep(10 * 1000)
    //nats1.close()
    Thread.sleep(5 * 60 * 1000)
    Await.result(pub, 100 seconds)
    Await.result(sub1, 100 seconds)
    Await.result(sub2, 100 seconds)
    nats.close()
    nats1.close()
    nats2.close()
    nats3.close()
    println(s"Consuming in count: ${ct.count}, max: ${(ct.max)/1000000.0}ms, min:${ct.min/1000000.0}ms, mean:${ct.mean/1000000.0}ms")
    println(s"Producing in count: ${pt.count}, max: ${(pt.max)/1000000.0}ms, min:${pt.min/1000000.0}ms, mean:${pt.mean/1000000.0}ms")
  }
}
