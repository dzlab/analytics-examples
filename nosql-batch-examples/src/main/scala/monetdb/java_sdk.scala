package monetdb

import java.sql.{DriverManager, Connection, PreparedStatement, ResultSet, ResultSetMetaData, Timestamp}
import scala.util.{Failure, Success, Try}
import common._
import nl.grons.metrics.scala._

object JavaSDK extends Instrumented {

  val MAX_BATCH_SIZE = 100

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")
  val at: Timer = metrics.timer("aggregas")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("localhost")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)
    val input = ops.i.get.getOrElse("")
    val bulk = ops.b.get.getOrElse(false)
    Class.forName("nl.cwi.monetdb.jdbc.MonetDriver") // load the driver
    val con: Connection = DriverManager.getConnection(s"jdbc:monetdb://$node/${anx.KEYSPACE}", "monetdb", "monetdb")
    Try {
      init()(con)
      if(!bulk)
        bt.time { upload(input, size)(con) } 
      else
        bt.time { bulk_upload(input)(con) } 
      query()(con)
      } match {
        case Success(future) => println("success")
        case Failure(t) => t.printStackTrace
    }
    con.close()
    println(s"Uploading data to MonetDB in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a MonetDB row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Aggregation on auctions in count: ${at.count}, max: ${(at.max)/1000000.0}ms, min:${at.min/1000000.0}ms, mean:${at.mean/1000000.0}ms")
  }

  def bulk_upload(input: String)(implicit con: Connection) {
    println(s"Executing a bulk loading from $input")
    execute(s"COPY INTO ${anx.TABLE_STD_FEED} from '$input' USING DELIMITERS '\t','\n' NULL AS '';", false)
  }

  def upload(input: String, size: Int)(implicit con: Connection) {
    val insert = anx.INSERT_STMT.replace(anx.KEYSPACE+"."+anx.TABLE_STD_FEED, anx.TABLE_STD_FEED) + ";" 
    //val batch = "START TRANSACTION;" + insert * size + "COMMIT;"
    val st: PreparedStatement = con.prepareStatement(insert) //batch)
    println(s"Executing batches of $size")

    val reader = new Reader(input)
    reader.consume(size, batch => {
      execute("START TRANSACTION;", false)
      batch.foreach(values => {
        var index = 1
        (anx.TYPES zip values).foreach{case (k, v) => {
          val obj: java.lang.Object = if(v=="NULL") k.zero else k.convert(v)
          k match {
            case `int`       => st.setInt(index, obj.asInstanceOf[java.lang.Integer])
            case `bigint`    => st.setLong(index, obj.asInstanceOf[java.lang.Long]) 
            case `bool`      => st.setBoolean(index, obj.asInstanceOf[java.lang.Boolean])
            case `double`    => st.setDouble(index, obj.asInstanceOf[java.lang.Double])
            case `timestamp` => st.setTimestamp(index, new Timestamp(obj.asInstanceOf[java.util.Date].getTime()))
            case `text`      => st.setString(index, obj.asInstanceOf[java.lang.String])
            case _ => println(s"Cannot convert $k:$v")
          }
          index += 1
        }}
        st.execute()//addBatch()
      })
      wt.time {execute("COMMIT;", false)}
    })
  }

  def init()(implicit con: Connection) {
    val rs: ResultSet = qt.time { con.createStatement().executeQuery(s"SELECT name FROM tables WHERE name like '${anx.TABLE_STD_FEED}'") }
    if(rs.next()) { 
      // if table exists then drop it
      qt.time { execute(s"DROP TABLE ${anx.TABLE_STD_FEED}", false)(con) } // reset
    }
    qt.time { execute(monetdb.TABLE_SCHEMA, false)(con) } // init
  }

  def query()(implicit con: Connection) {
    println("Executing queries : ")
    qt.time { execute(s"SELECT COUNT(*) FROM ${anx.TABLE_STD_FEED}", true)(con) } // query
    println("Executing aggregas: ")
    at.time { execute(s"SELECT buyer_member_id, SUM(buyer_bid) FROM ${anx.TABLE_STD_FEED} GROUP BY buyer_member_id", true) }
    at.time { execute(s"SELECT is_click, COUNT(auction_id_64) FROM ${anx.TABLE_STD_FEED} GROUP BY is_click", true) }
    at.time { execute(s"SELECT media_type, SUM(buyer_bid) FROM ${anx.TABLE_STD_FEED} GROUP BY media_type", true) }
  }

  def execute(query: String, withResult: Boolean)(implicit con: Connection) {
    val st = con.createStatement()  
    if(!withResult) {
      st.execute(query)
      return
    }
    val rs: ResultSet = st.executeQuery(query)
    //if(rs.isAfterLast()) return // no rows 
    val md: ResultSetMetaData = rs.getMetaData()
    (1 to md.getColumnCount()).foreach(i => print(md.getColumnName(i) + ":" + md.getColumnTypeName(i) + "\t"))
    println("-------")  
    while (rs.next()) {
      (1 to md.getColumnCount()).foreach(j => print(rs.getString(j) + "\t"))
      println("")
    }
  }
}
