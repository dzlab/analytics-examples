package cassandra

import scala.util.{Try, Success, Failure}
import java.io.{BufferedReader, File, FileReader}
import java.nio.ByteBuffer
import java.util.Comparator
import org.rogach.scallop._
import org.apache.cassandra.config.Config
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter
import org.apache.cassandra.db.marshal.{AsciiType, AbstractType}
import org.apache.cassandra.dht.Murmur3Partitioner
import org.apache.cassandra.utils.{ByteBufferUtil, UUIDGen}//, JVMStabilityInspector}
import nl.grons.metrics.scala._
import com.github.tototoshi.csv.CSVReader
import org.apache.cassandra.io.sstable.format.SSTableWriter
import common._
//import org.apache.cassandra.io.sstable.SSTableWriter

class UOptions(args: Array[String]) extends ScallopConf(args) {
  val i = opt[String](required = true, descr = "Input auctions (i.e. standard_feed) file")
  val c = opt[Boolean](required = false, descr = "Create anx tables")
  val o = opt[String](required = false, descr = "Output directory where Cassandra tables will be stored")
  val p = opt[Int](required = false, descr= "Cassandra port number")
}

object SSTableUnsorted extends Instrumented {
  val bt: Timer = metrics.timer("batch")
  val ct: Timer = metrics.timer("conversion")
  val wt: Timer = metrics.timer("write")
 
  def main(args: Array[String]) {
    val options = new UOptions(args)
    (options.i.get, options.o.get) match {
      case (Some(i), Some(o))  => {
        bt.time{Try(upload(i, o))} match {
          case Success(future) => 
          case Failure(t) => t.printStackTrace; //JVMStabilityInspector.inspectThrowable(t)
        }
      }
      case _ => println("doing nothing")
    }
    println(s"Creating CASSANDRA SSTable in max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Writing a CASSANDRA row in max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
  } 

  // https://gist.github.com/abhijitchanda/3774733
  // xml to https://github.com/FasterXML/jackson-dataformat-avro
  def upload(input: String, output: String) {
    Config.setClientMode(true)
    val reader: CSVReader = CSVReader.open(new File(input))(tsv)
    val keys = anx.COLUMNS.map(c => ByteBufferUtil.bytes(c))
    // prepare the SSTable writer
    val directory: File = new File(output);
    if (!directory.exists())
      directory.mkdir()
    val partitioner = new Murmur3Partitioner()
    val keyspace = "anx"
    val columnFamily = "standard_feed"
    val comparator = AsciiType.instance
    val subComparator = null.asInstanceOf[AbstractType[_]]
    val bufferSizeInMB = 128//.asInstanceOf[java.lang.Integer.TYPE]
    val writer: SSTableSimpleUnsortedWriter = new SSTableSimpleUnsortedWriter(directory, partitioner, keyspace, columnFamily, comparator, subComparator, bufferSizeInMB)
    checkProperties(writer) 
    // write data to output
    val zero = ByteBufferUtil.bytes("0")
    var index = 0
    reader.iterator.foreach(values => {
      val uuid: ByteBuffer = ByteBufferUtil.bytes(values(0));
      val datetime = timestamp.convert(values(1)).getTime
      //println(s"writing value $values")
      wt.time {
      writer.newRow(uuid)
      (keys.tail zip values.tail).foreach{ case (k, v) => {
        val bytes = if(v == "NULL") zero else ByteBufferUtil.bytes(v)
        require(datetime != null, "datetime is null")
        require(bytes != null, "bytes is null")
        require(k != null, "key is null")
        writer.addColumn(k, bytes, datetime)
      }}
      println(s"finished handling row at $index")
      index = index + 1
      }
    })
    reader.close
    writer.close
  }

  def checkProperties(writer: SSTableSimpleUnsortedWriter) {
    val cls = classOf[org.apache.cassandra.io.sstable.AbstractSSTableSimpleWriter]
    //val cls = classOf[org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter]
    val method = cls.getDeclaredMethod("getWriter")
    method.setAccessible(true)
    val wr: SSTableWriter = method.invoke(writer).asInstanceOf[SSTableWriter]
    require(wr != null, "SSTable writer is null")
  }
}
