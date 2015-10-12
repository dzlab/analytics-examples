package riak

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.{DeleteValue, FetchValue, StoreValue}
import com.basho.riak.client.core.{RiakCluster, RiakNode}
import com.basho.riak.client.core.query.{Location, Namespace, RiakObject}
import com.basho.riak.client.core.util.BinaryValue

import scala.util.{Failure, Success, Try}
import common._
import nl.grons.metrics.scala._

object JavaSDK extends Instrumented {

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")
  val at: Timer = metrics.timer("aggregas")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("127.0.0.1")
    val port = ops.p.get.getOrElse(10017)
    val input = ops.i.get.getOrElse("")

    val cluster: RiakCluster = setUpCluster(node, port)
    val client: RiakClient = new RiakClient(cluster)
    Try { 
      bt.time { upload(input)(client) } 
      } match {
        case Success(future) => println("success")
        case Failure(t) => t.printStackTrace
    }
    cluster.shutdown()
    println(s"Uploading data to Aerospike in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a Aerospike row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Aggregation on  auctions in count: ${at.count}, max: ${(at.max)/1000000.0}ms, min:${at.min/1000000.0}ms, mean:${at.mean/1000000.0}ms")
  }

  def upload(input: String)(implicit client: RiakClient) {
    val convertrs = (anx.COLUMNS zip anx.TYPES).toMap
    val reader = new common.Reader(input)
    reader.consume(values => {
      (anx.COLUMNS zip values).foreach { case(k, v) => {
        val convertr = convertrs.getOrElse(k, text)
        val bytes = convertr.toBytes(v).array()
        val obj: RiakObject = new RiakObject().setValue(BinaryValue.create(bytes))
        val location: Location = new Location(new Namespace(anx.TABLE_STD_FEED), k);
        val store: StoreValue = new StoreValue.Builder(obj).withLocation(location).build();
        wt.time { client.execute(store) }
      }}
    })
    /*
    // execute operation
    val response: StoreValue.Response = client.execute(storeOp)
    // read created object
    val fetchOp: FetchValue = new FetchValue.Builder(quoteObjectLocation)
      .build()
    val fetchedObject: RiakObject = client.execute(fetchOp).getValue(classOf[RiakObject]);
    require(fetchedObject.getValue.equals(quoteObject.getValue()))*/
  }
  
  def setUpCluster(host: String, port: Int): RiakCluster = {
    val node: RiakNode = new RiakNode.Builder()
      .withRemoteAddress(host)
      .withRemotePort(port)
      .build();

    // This cluster object takes our one node as an argument
    val cluster: RiakCluster = new RiakCluster.Builder(node).build()
                                                            
    // The cluster must be started to work, otherwise you will see errors
    cluster.start()
    cluster
  }

}
