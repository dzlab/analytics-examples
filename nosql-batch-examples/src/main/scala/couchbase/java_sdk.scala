package couchbase

import scala.util.{Try, Failure, Success}
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import nl.grons.metrics.scala._
import common._

//import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._


object JavaSDK extends Instrumented {

  val MAX_BATCH_SIZE = 100

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")

 
  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("localhost")
    val input= ops.i.get.getOrElse("")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)

    val cluster: Cluster = CouchbaseCluster.create(node)
    val bucket: Bucket = cluster.openBucket("anx")

    Try{bt.time {upload(input, size)(cluster, bucket)}; qt.time{ query()(cluster, bucket)} } match {
      case Success(future) => println("success")
      case Failure(t) => t.printStackTrace
    }
    cluster.disconnect()
    println(s"Uploading data to Couchbase in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a Couchbase row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  }

  def upload(input: String, size: Int)(implicit cluster: Cluster, bucket: Bucket) {
    val reader = new common.Reader(input)
    reader.consume(values => {
      wt.time{ bucket.upsert(CouchbaseUtils.toJson(values)) }
    })
  }

  def query()(implicit cluster: Cluster, bucket: Bucket) {
    val auction = bucket.get("4139178651222865337")
    println("Found: \n" + auction)
    cluster.disconnect()
  }

}
