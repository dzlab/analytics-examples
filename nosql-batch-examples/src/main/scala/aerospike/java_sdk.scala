package aerospike

import com.aerospike.client.{AerospikeClient, AerospikeException}
import com.aerospike.client.{Bin, Key, Record}
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.listener.WriteListener;
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Try, Success, Failure}
import nl.grons.metrics.scala._
import common._

// resources: http://www.aerospike.com/docs/client/java/
object JavaSDK extends Instrumented {
 
  val MAX_BATCH_SIZE: Int = 100

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("localhost")
    val port = ops.p.get.getOrElse(3000)
    val input= ops.i.get.getOrElse("")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)

    val policy: AsyncClientPolicy = new AsyncClientPolicy()
    val client: AsyncClient = new AsyncClient(policy, node, port)

    Try{bt.time {upload(input, size)(client, policy)}; Thread.sleep(10000L); qt.time{ query()(client)} } match {
      case Success(future) => println("success")
      case Failure(t) => t.printStackTrace
    }
    client.close()
    println(s"Uploading data to Aerospike in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a Aerospike row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  }

  def upload(input: String, size: Int)(implicit client: AsyncClient, policy: AsyncClientPolicy) {
    val handler = new WriteHandler(size)
    val writePolicy = policy.asyncWritePolicyDefault
    val reader = new common.Reader(input)
    reader.consume(values => {
      val key: Key = new Key(anx.KEYSPACE, "standard_feed", values(0))
      val bins: Array[Bin] = (anx.COLUMNS zip values).map {case(k, v) => new Bin(if(k.length<=14) k else k.substring(0, 14), v) } .toArray
      wt.time { client.put(writePolicy, handler, key, bins:_*) }
    })
  }

  def query()(client: AsyncClient) {
    val key: Key = new Key(anx.KEYSPACE, "standard_feed", "4419699335796603740")
    val record: Record = client.get(null, key)
    println(record)
  }

  def sample() {
    val client: AerospikeClient = new AerospikeClient("localhost", 3000);

    val key: Key = new Key("test", "demo", "putgetkey");
    val bin1: Bin = new Bin("bin1", "value1");
    val bin2: Bin = new Bin("bin2", "value2");

    // Write a record
    client.put(null, key, bin1, bin2);
    
    // Read a record
    val record: Record = client.get(null, key);
    
    client.close()
  }

}

class WriteHandler(val max: Int) extends WriteListener {
  val count: AtomicInteger = new AtomicInteger()

  def onSuccess(key: Key) {
    /*val rows: Int = count.incrementAndGet();
          
    if (rows == max) {
      println(s"Successfully inserted $max elements")
      count.set(0)
    }*/
  }

  def onFailure(e: AerospikeException) {
    println("Failed to push data to Server")
    e.printStackTrace
  }
}
