package monetdb

import common._
import common.Converters._
import scala.util.{Failure, Success, Try}
import nl.grons.metrics.scala._
import java.io.{File, OutputStream, BufferedOutputStream, FileOutputStream}
import java.sql.ResultSet

object TableWriter extends Instrumented {

  val TABLES_DIR = "/mnt/bachir/monetdb" //System.getProperty("java.io.tmpdir") + "/monetdb-table/" 

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")
  val at: Timer = metrics.timer("aggregas")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val input = ops.i.get.getOrElse("")
    val node = ops.n.get.getOrElse("localhost")
    cleanup()
    val client = JdbcClient(s"jdbc:monetdb://$node/${anx.KEYSPACE}", "monetdb", "monetdb")
    Try {
      init()(client)
      generate(input)
      upload(node)(client)
      query()(client)
    } match {
      case Success(future) =>  println("success")
      case Failure(e) => e.printStackTrace
    }
    client.close
    cleanup()
    println(s"Uploading data to MonetDB in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a MonetDB row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Aggregation on auctions in count: ${at.count}, max: ${(at.max)/1000000.0}ms, min:${at.min/1000000.0}ms, mean:${at.mean/1000000.0}ms")
  }
  
  def cleanup() {
    // delete previous table files
    (0 to 93).foreach (i => {
      val filename = TABLES_DIR + "/col" + i + ".bulkload"
      val file = new File(filename)
      if(file.exists) file.delete()
    })
  }
  def init()(implicit client: JdbcClient) {
    // initialize table schema
    val rs: ResultSet = qt.time { client.execute(s"SELECT name FROM tables WHERE name like '${anx.TABLE_STD_FEED}'") }
    if(rs.next()) { 
      // if table exists then drop it
      qt.time { client.execute(s"DROP TABLE ${anx.TABLE_STD_FEED}", false) } // reset
    }
    qt.time { client.execute(monetdb.TABLE_SCHEMA, false) } // init
  }

  def open(): (OutputStream, Seq[OutputStream]) = {
    new File(TABLES_DIR).mkdirs()
    val writers = (0 to 93).map(i => {
      val filename = TABLES_DIR + "/col" + i + ".bulkload"
      //new File(filename).getParentFile().mkdirs()
      new BufferedOutputStream(new FileOutputStream(filename))
    })
    (writers.head, writers.tail)
  }
  
  def close(writers: Seq[OutputStream]) {
    writers.foreach(writer => writer.close)
  }

  def upload(node: String)(implicit client: JdbcClient) {
    println("Uploading generated tables to MonetDB")
    val tables = (0 to 93).map(i => {
      val filename = TABLES_DIR + "/col" + i + ".bulkload"
      s"'$filename'"
    }).reduce(_ + ", " + _)
    val query = s"COPY BINARY INTO ${anx.TABLE_STD_FEED} FROM ($tables);"
    println(s"Executing $query")
    bt.time { client.execute(query, false) }
  }

  def query()(implicit client: JdbcClient) {
    println("Executing queries : ")
    qt.time { client.execute(s"SELECT COUNT(*) FROM ${anx.TABLE_STD_FEED}", true) } // query
    println("Executing aggregas: ")
    at.time { client.execute(s"SELECT buyer_member_id, SUM(buyer_bid) FROM ${anx.TABLE_STD_FEED} GROUP BY buyer_member_id", true) }
    at.time { client.execute(s"SELECT is_click, COUNT(auction_id_64) FROM ${anx.TABLE_STD_FEED} GROUP BY is_click", true) }
    at.time { client.execute(s"SELECT media_type, SUM(buyer_bid) FROM ${anx.TABLE_STD_FEED} GROUP BY media_type", true) }
  }

  def generate(input: String) {
    println("Generating MonetDB tables")
    val (primary, writers) = open()
    val reader = new Reader(input)
    var key = 1
    val col0 = new IntegerValueConverter()
    reader.consume(values => {
      primary.write(col0.convert(key))
      //primary.write(int.toBytes(auto.toString).array())
      key = key + 1
      val it1 = anx.CONVERTERS.iterator 
      val it2 = anx.TYPES.map(x => if(x==timestamp) date else x).iterator
      (values zip writers).foreach{case(v, w) => {
        val c = it1.next
        val t = it2.next
        val res = t.convert(v)
        val bytes = c.convert(if(res==null) t.zero else res) //t.toBytes(v).array()
        println(s"Converted '$v' using '$t' into '$res' in bytes '${bytes.mkString}' which is '${t.fromBytes(bytes)}'")
        w.write(bytes)
      }};println("-----------------------")
    })
    close(List(primary))
    close(writers)
  }
}

