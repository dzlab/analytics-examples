package couchbase

import common._
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import scala.util.{Try, Success, Failure}
import nl.grons.metrics.scala._

object Spark extends Instrumented {
  
  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val input = ops.i.get.getOrElse(null)
    require(input!=null && !input.isEmpty, "Non valid input file")

    // Configure Spark
    val cfg = new SparkConf()
      .setAppName("couchbaseQuickstart") // give your app a name
      .setMaster("local[*]") // set the master to local for easy experimenting
      .set("com.couchbase.bucket.anx", "") // open the travel-sample bucket
    
    // Generate The Context
    val sc = new SparkContext(cfg)
    Try{bt.time{ upload(input)(sc) }; qt.time{ query()(sc)};} match {
      case Success(future) => println("success")
      case Failure(t) => t.printStackTrace
    }
    println(s"Ingesting to Couchbase in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    //println(s"Converting a value to bytes in max: ${(ct.max)/1000000.0}ms, min:${ct.min/1000000.0}ms, mean:${ct.mean/1000000.0}ms")
    println(s"Querying from Couchbase in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  }

  def upload(filename: String)(implicit sc: SparkContext) {
    val rowRDD = sc.textFile(filename).map{row =>
        val obj = JsonObject.create()
        (anx.COLUMNS zip row.split("\t")).foreach{case(k, v) => obj.put(k, if(v == "NULL") 0 else v)}
        JsonDocument.create(obj.get("auction_id_64").asInstanceOf[String], obj)
      } 
      // save standard feeds to cassandra
      .saveToCouchbase()
  }

  def query()(implicit sc: SparkContext) {
    sc.parallelize(Seq("4139178651222865337", "4419699335796603740")) // Define Document IDs
      .couchbaseGet[JsonDocument]() // Load them from Couchbase
      .map(_.content()) // extract the content
      .collect() // collect all data
      .foreach(println) // print it out
  }
}
