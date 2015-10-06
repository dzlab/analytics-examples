package cassandra
/* works for cassandra 2.2.0

import java.io.File
import java.net.InetAddress 
import javax.net.ssl.SSLContext

import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.utils.{NativeSSTableLoaderClient, OutputHandler, JVMStabilityInspector}
import org.apache.cassandra.config.EncryptionOptions
import org.apache.cassandra.tools.BulkLoadConnectionFactory 
import org.apache.cassandra.security.SSLFactory
import org.apache.cassandra.streaming.StreamConnectionFactory
import org.apache.cassandra.streaming.StreamResultFuture
import com.datastax.driver.core.SSLOptions

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}

import org.rogach.scallop._

class Options(args: Array[String]) extends ScallopConf(args) {
  val i = opt[String](required = true, descr = "Input directory containing auctions (i.e. tandard_feed) file")
  val c = opt[Boolean](required = false, descr = "Create anx tables")
  val o = opt[String](required = false, descr = "Output directory where Cassandra tables will be stored")
  val p = opt[Int](required = false, descr= "Cassandra port number")
}

object SSTableLoaderSimple extends Instrumented {
  
  def main(args: Array[String]) {
    val options = new Options(args)
    (options.c.get, options.i.get, options.p.get) match {
      case (Some(c), Some(i), Some(p)) if(c==true) => {
        upload(i, null, p, true, true, null)
      }
      case _ => println("doing nothing")
    }
  } 

  def upload(directory: String, hosts: Set[InetAddress], port: Int, verbose: Boolean, debug: Boolean, ignores: Set[InetAddress]) {
    // create a table loader
    val client = new ExternalClient(hosts, port, "", "", 7000, 7001, new EncryptionOptions.ServerEncryptionOptions(),  buildSSLOptions())
    val outputHandler = new OutputHandler.SystemOutput(verbose, debug);
    val connectionsPerHost: Int = 1
    val loader = new SSTableLoader(new File(directory), client, outputHandler, connectionsPerHost)
    // stream data to cassandra
    val trial: Try[StreamResultFuture] = Try(loader.stream(ignores.asJava))
    trial match {
      case Success(future) => println(future.get())
      case Failure(t) => JVMStabilityInspector.inspectThrowable(t); println(t);
    }
  }

  def buildSSLOptions(): SSLOptions = {
    //if (!clientEncryptionOptions.enabled) return null
    val clientEncryptionOptions = new EncryptionOptions.ClientEncryptionOptions()
    val trial: Try[SSLContext] = Try(SSLFactory.createSSLContext(clientEncryptionOptions, true))
    trial match {  
      case Success(sslContext) => return new SSLOptions(sslContext, clientEncryptionOptions.cipher_suites)
      case Failure(e) => throw new RuntimeException("Could not create SSL Context.", e);
    }
  }
}

class ExternalClient(hosts: Set[InetAddress], port: Int, user: String, passwd: String, storagePort: Int, sslStoragePort: Int, serverEncOptions: EncryptionOptions.ServerEncryptionOptions, sslOptions: SSLOptions) extends NativeSSTableLoaderClient(hosts.asJava, port, user, passwd, sslOptions) {
  override def getConnectionFactory(): StreamConnectionFactory = {
    new BulkLoadConnectionFactory(storagePort, sslStoragePort, serverEncOptions, false);
  }
}
*/
