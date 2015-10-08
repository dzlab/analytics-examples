package aerospike

import com.aerospike.client.{AerospikeClient, AerospikeException}
import com.aerospike.client.{Bin, Key, Language, Record}
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.lua.LuaConfig
import com.aerospike.client.query.{ResultSet, Statement}
import java.io.File
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
  val at: Timer = metrics.timer("aggregas")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("localhost")
    val port = ops.p.get.getOrElse(3000)
    val input= ops.i.get.getOrElse("")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)
    val init = ops.c.get.getOrElse("")

    val policy: AsyncClientPolicy = new AsyncClientPolicy()
    val client: AsyncClient = new AsyncClient(policy, node, port)

    Try{
      if(!init.isEmpty) register(init)(client)
      bt.time {upload(input, size)(client, policy)}; 
      Thread.sleep(10000L); 
      query()(client) 
      } match {
      case Success(future) => println("success")
      case Failure(t) => t.printStackTrace
    }
    client.close()
    println(s"Uploading data to Aerospike in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a Aerospike row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Aggregation on  auctions in count: ${at.count}, max: ${(at.max)/1000000.0}ms, min:${at.min/1000000.0}ms, mean:${at.mean/1000000.0}ms")
  }
  
  def register(clientPath: String)(implicit client: AsyncClient) {
    println(s"Registering UDF in $clientPath")
    //val clientPath = new File(".").getAbsolutePath + "udf/aggregations.lua"
    //val clientPath = Thread.currentThread().getContextClassLoader().getResource("udf/aggregations.lua").toExternalForm()//getPath()
    client.register(null, clientPath, "aggregations.lua", Language.LUA)
    LuaConfig.SourceDirectory = "udf"
  }

  def upload(input: String, size: Int)(implicit client: AsyncClient, policy: AsyncClientPolicy) {
    val handler = new WriteHandler(size)
    val writePolicy = policy.asyncWritePolicyDefault
    val convertrs = (anx.COLUMNS zip anx.TYPES).toMap
    val reader = new common.Reader(input)
    reader.consume(values => {
      val key: Key = new Key(anx.KEYSPACE, anx.TABLE_STD_FEED, values(0))
      val bins: Array[Bin] = (anx.COLUMNS zip values).map {case(k, v) => {
        val convertr = convertrs.getOrElse(k, text)
        new Bin(if(k.length<=14) k else k.substring(0, 14), convertr.convert(v)) 
      }} .toArray
      wt.time { client.put(writePolicy, handler, key, bins:_*) }
    })
  }

  def get(auctionId: String)(implicit client: AsyncClient): Record = {
    val key: Key = new Key(anx.KEYSPACE, anx.TABLE_STD_FEED, "4419699335796603740")
    val record: Record = client.get(null, key)
    record
  }

  def query()(client: AsyncClient) {
    // query head line
    println(qt.time { get("4419699335796603740")(client) })
    // query tail line
    println(qt.time { get("7227797695851521841")(client) })
    
    val stmt: Statement = new Statement()
    stmt.setNamespace(anx.KEYSPACE);
    stmt.setSetName(anx.TABLE_STD_FEED);
    //stmt.setFilters( Filter.range("baz", 0,100) );
    stmt.setBinNames("auction_id_64");

    val rs: ResultSet = at.time { client.queryAggregate(null, stmt, "aggregations", "count") } 
    if (rs.next()) {
      val result: Object = rs.getObject();
      println("Count = " + result);
    }
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
