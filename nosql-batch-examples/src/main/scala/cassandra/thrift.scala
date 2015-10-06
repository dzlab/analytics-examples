package cassandra

import java.io.File
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{Cassandra, Mutation}
import org.apache.cassandra.thrift.{CfDef, KsDef, Column}
import org.apache.cassandra.thrift.{ColumnDef, ColumnParent, ColumnPath, ColumnOrSuperColumn}
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.InvalidRequestException
import org.apache.cassandra.thrift.NotFoundException
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.TimedOutException
import org.apache.cassandra.thrift.UnavailableException
import org.apache.thrift.TException
import common._
import nl.grons.metrics.scala._
import com.github.tototoshi.csv.CSVReader
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import org.rogach.scallop._

class TOptions(args: Array[String]) extends ScallopConf(args) {
  val i = opt[String](required = false, descr = "Input auctions (i.e. standard_feed) file")
  val c = opt[Boolean](required = false, descr = "A flag indicating whether to create a new keyspace or not")
  val s = opt[Int](required = false, descr = "Size of the rows batch")
  val n = opt[String](required = false, descr = "The hostname:port where cassandra is running")
}

object ThriftSample extends Instrumented {
  type JM[K, V] = java.util.Map[K, V]
  type JHM[K, V] = java.util.HashMap[K, V]
  type JL[T] = java.util.List[T]
  type JAL[T] = java.util.ArrayList[T]
  type JS = java.lang.String

  val bt: Timer = metrics.timer("batch")
  val ct: Timer = metrics.timer("conversion")
  val wt: Timer = metrics.timer("write")

  val KEYSPACE: String = "anx"
  val TABLE: String = "auctions"
  val BATCH_SIZE = 10000

  lazy val tr:CClient = new CClientBuilder().withHost("ec2-52-27-194-206.us-west-2.compute.amazonaws.com").withPort(9160).build()

  lazy val client = tr.client

  lazy val keys = anx.COLUMNS.map( c => ByteBuffer.wrap(c.getBytes("UTF-8")))

  def main(args: Array[String]) {
    connect 
    val options = new TOptions(args)
    options.c.get match {
      case Some(c) => createSchema 
      case None => println("doing nothing with -c")
    }
    Try((options.i.get, options.s.get) match {
      case (Some(i), Some(s)) => {
        bt.time{upload(i, s)} 
      }
      case (Some(i), None) => bt.time {upload(i, BATCH_SIZE)} 
      case _ => println("doing nothing with -i")
    }) match {
      case Success(future) => println("success"); read
      case Failure(t) => t.printStackTrace; //JVMStabilityInspector.inspectThrowable(t)
    }
    disconnect
    println(s"Uploading data to CASSANDRA in max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Converting a value to bytes in max: ${(ct.max)/1000000.0}ms, min:${ct.min/1000000.0}ms, mean:${ct.mean/1000000.0}ms")
    println(s"Writing a CASSANDRA row in max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  }

  // source: https://wiki.apache.org/cassandra/ThriftExamples#Java 
  def upload(input: String, size: Int) {
    println(s"Uploading data from $input in batches of $size rows")
    val reader: CSVReader = CSVReader.open(new File(input))(tsv)
    // insert data
    client.set_keyspace("anx")
    val parent: ColumnParent = new ColumnParent(TABLE)
    // write to output
    val empty = ByteUtil.EMPTY_BYTE_BUFFER
    reader.iterator.sliding(size, size).foreach(batch => {
      val job: JM[ByteBuffer, JM[JS, JL[Mutation]]] = new JHM[ByteBuffer, JM[JS, JL[Mutation]]]()
      batch.foreach(values => {
        val uuid: ByteBuffer = bigint.toBytes(values(0));
        val datetime = timestamp.convert(values(1)).getTime
        val types = anx.TYPES.iterator; types.next
        //val mutations: java.util.List[Mutation] = new java.util.ArrayList[Mutation]()
        val cfMap: JM[JS, JL[Mutation]] = new JHM[JS, JL[Mutation]]()
        //wt.time {
          val mutations: java.util.List[Mutation] = (keys.tail zip values.tail).map{ case (k, v) => {
            val t = types.next 
            val bytes = if(v == "") empty else if(v == "NULL") t.zeroAsBytes else ct.time {t.toBytes(v)}
            CUtils.createMutation(k, bytes, datetime)
            //client.insert(uuid, parent, column, ConsistencyLevel.ONE)
          }}.asJava
          cfMap.put(TABLE, mutations)
          job.put(uuid, cfMap)
        //}
      })
      wt.time { client.batch_mutate(job, ConsistencyLevel.ONE) }
    })
  }
  
  def read() {
    println("reading some data")
    val auction_id_64: String = "4139178651222865337"
    // read single column
    println("Reading a single column");
    val path: ColumnPath = new ColumnPath(TABLE);
    path.setColumn(text.toBytes("datetime"));
    println(client.get(bigint.toBytes(auction_id_64), path, ConsistencyLevel.ONE));
    // read entire row
    println("Reading an entire row")
    val predicate: SlicePredicate = new SlicePredicate();
    val sliceRange: SliceRange = new SliceRange(text.toBytes(""), text.toBytes(""), false, 10);
    predicate.setSlice_range(sliceRange);
    val parent: ColumnParent = new ColumnParent(TABLE)
    val results: java.util.List[ColumnOrSuperColumn] = client.get_slice(bigint.toBytes(auction_id_64), parent, predicate, ConsistencyLevel.ONE);
    for (result: ColumnOrSuperColumn <- results.asScala) {
      val column: Column = result.column;
      println(column);
      //System.out.println(toString(column.name) + " -> " + toString(column.value));
    }
  }

  def createSchema() {
    println("create a new keyspace")
    // check if keyspace exists
    tr.dropKeyspace(KEYSPACE)
    // column families
    val cfs: java.util.List[CfDef] = new java.util.ArrayList[CfDef]();
    val cf: CfDef = new CfDef()
    cf.setKeyspace(KEYSPACE)
    cf.setName(TABLE)
    cf.setColumn_type("Standard")
    cf.setComparator_type("BytesType")
    (keys zip anx.TYPES).map(_ match { case (k, t) =>
      val tt = if (t==int) "Int32Type" else if(t==bigint) "LongType" else if (t==double) "DoubleType" else if (t==timestamp) "TimestampType" else if(t==text) "UTF8Type" else "BytesType"
      cf.addToColumn_metadata(new ColumnDef(k, tt))
    })
    cfs.add(cf);
    // replication strategy
    val stratOpts: java.util.Map[String,String] = new java.util.HashMap[String,String]();
    stratOpts.put("replication_factor", "1")

    // keyspace
    val cfsKs: KsDef = new KsDef()
        .setName(KEYSPACE)
        .setStrategy_class(classOf[org.apache.cassandra.locator.SimpleStrategy].getName)
        .setStrategy_options(stratOpts)
        .setDurable_writes(false)
        .setCf_defs(cfs);
    client.system_add_keyspace(cfsKs)
  }

  def connect = tr.open()
  def disconnect = tr.close()
}
