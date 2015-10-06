package cassandra

import java.nio.ByteBuffer
import org.apache.thrift.transport.{TFramedTransport, TTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.cassandra.thrift._
import scala.util.{Try, Success, Failure}
 
class CClientBuilder {
  var host: String = "localhost"
  var port: Int = 9160

  def withHost(host: String): CClientBuilder = {
    require(host!=null && !host.isEmpty, s"Non valid host name $host")
    this.host = host
    this
  }

  def withPort(port: Int): CClientBuilder = {
    require(port>0, s"Non valid port number $port")
    this.port = port
    this
  }

  def build(): CClient = {
    println(s"Creating Cassandra client with $host:$port")
    val tr: TTransport = new TFramedTransport(new TSocket(host, port))
    new CClient(tr)
  }
}

class CClient(val tr: TTransport) {
  def open() = Try(tr.open) match {
    case Success(future) => //println("")
    case Failure(t) => println("Failed to open connection to cassandra $host:$port"); throw t 
  }

  lazy val client: Cassandra.Client = {
    val proto: TProtocol = new TBinaryProtocol(tr)
    val client: Cassandra.Client = new Cassandra.Client(proto);
    client
  }

  def dropKeyspace(keyspace: String) = {
    // check if keyspace exists
    try {
      val cfsKs: KsDef = client.describe_keyspace(keyspace);
      if(cfsKs!=null && cfsKs.getName().equals(keyspace)) 
        client.system_drop_keyspace(keyspace);//return;
    }catch {
      case (nfe: org.apache.cassandra.thrift.NotFoundException) => println("Keyspace not found")
    }
  }
  def close() = tr.close
}

object CUtils {

  def createMutation(colName: ByteBuffer, colValue: ByteBuffer, timestamp: Long): Mutation = {
    val col:Column = new Column(colName)
    col.setValue(colValue)
    col.setTimestamp(timestamp)

    val cosc: ColumnOrSuperColumn = new ColumnOrSuperColumn()
    cosc.setColumn(col)

    val mutation: Mutation = new Mutation()
    mutation.setColumn_or_supercolumn(cosc)

    mutation
  }
}
