package cassandra

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy, DefaultRetryPolicy}
import com.datastax.driver.core.{PreparedStatement, Session, Cluster, BoundStatement}
import scala.util.{Try, Success, Failure}
import nl.grons.metrics.scala._
import common._

object Driver extends Instrumented {
  
  val MAX_BATCH_SIZE = 704
  
  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")

  def main(args: Array[String]) {
    val ops = new TOptions(args)
    val node = ops.n.get.getOrElse("localhost")
    val input= ops.i.get.getOrElse("")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)
    
    Try(bt.time {upload(node, input, size)}) match {
      case Success(future) => println("success")
      case Failure(t) => t.printStackTrace
    }

    println(s"Uploading data to CASSANDRA in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    //println(s"Converting a value to bytes in max: ${(ct.max)/1000000.0}ms, min:${ct.min/1000000.0}ms, mean:${ct.mean/1000000.0}ms")
    println(s"Writing a CASSANDRA row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  }

  def upload(node: String, input: String, size: Int) {
    val cluster: Cluster = Cluster
      .builder() 
      .addContactPoint(node)
      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
      .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()
    val session: Session = cluster.connect()

    session.execute(anx.DROP_KEYSPACE_STMT(anx.KEYSPACE))
    session.execute(anx.CREATE_KEYSPACE_STMT(anx.KEYSPACE))
    session.execute(anx.SCHEMA)

    val now = System.currentTimeMillis()

    val batch = "BEGIN BATCH USING TIMESTAMP " + now + " " + (anx.INSERT_STMT + "; ") * size + "APPLY BATCH"
    
    val insertStmt: PreparedStatement = session.prepare(batch)
    val reader = new Reader(input)
    reader.consume(size, batch => {
      val boundStmt: BoundStatement = insertStmt.bind()
      var index = 0
      batch.foreach(values => {
        (anx.TYPES zip values).foreach{case (k, v) => {
          val obj: java.lang.Object = if(v=="NULL") k.zero else k.convert(v)
          k match {
            case `int` => boundStmt.setInt(index, obj.asInstanceOf[java.lang.Integer])
            case `bigint` => boundStmt.setLong(index, obj.asInstanceOf[java.lang.Long]) 
            case `bool` => boundStmt.setBool(index, obj.asInstanceOf[java.lang.Boolean])
            case `double` => boundStmt.setDouble(index, obj.asInstanceOf[java.lang.Double])
            case `timestamp` => boundStmt.setDate(index, obj.asInstanceOf[java.util.Date])
            case `text` => boundStmt.setString(index, obj.asInstanceOf[java.lang.String])
            case _ => println(s"Cannot convert $k:$v")
          }
          require(boundStmt.isSet(index), s"Value at $index is not set")
          index += 1
        }}
      })
      wt.time {session.execute(boundStmt)}
    })
    cluster.close()
  }
}
