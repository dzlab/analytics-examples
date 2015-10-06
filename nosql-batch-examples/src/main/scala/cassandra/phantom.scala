package cassandra

//import com.websudos.phantom.dsl._
import java.io.File
import nl.grons.metrics.scala._
import com.github.tototoshi.csv.{CSVReader, TSVFormat}
import org.apache.cassandra.config.Config
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.cql3.ColumnSpecification
import scala.collection.convert.decorateAll._
import common._

object PhantomApp extends Instrumented {
  val bt: Timer = metrics.timer("batch")
  val ct: Timer = metrics.timer("conversion")
  val wt: Timer = metrics.timer("write")

  def main(args: Array[String]) {
    val appConf = new AppConf(args)
    appConf.c.get.map(create => if(create==true) println ("not implmented"))
    appConf.auctions.get.map(input => {
      appConf.o.get match {
        case Some(output) => bt.time {batch_upload(input, output)}
        case None => println("No output directory for cassandra table was specified.")
      }
    })
    println(s"Batch ingestion to CASSANDRA in max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Writing rows to SSTable in max: ${wt.max/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Converting to JAVA types in max: ${ct.max/1000000.0}ms, min:${ct.min/1000000.0}ms, mean:${ct.mean/1000000.0}ms")
  }

  def batch_upload(input: String, output: String) {
    Config.setClientMode(true);
    val writer = CQLSSTableWriter.builder
      .inDirectory(output) // set the output directory
      .forTable(anx.SCHEMA)       // set the table schema
      .using(anx.INSERT_STMT)     // set the insert statement
      .build()
    val reader: CSVReader = CSVReader.open(new File(input))(tsv)
    val zeros: Map[Converter, java.lang.Object] = Map((int, int.convert("0")), (double, double.convert("0")), (bigint, bigint.convert("0")), (text, text.convert("0")), (bool, bool.convert("0")))
    val zero = java.lang.Integer.valueOf(0)
    reader.iterator.foreach(values => { 
      val list = new java.util.ArrayList[java.lang.Object](values.size);
      (values zip anx.TYPES).foreach { case (v, t) => list.add(if(v == "NULL") ct.time{zeros.getOrElse(t, zero)} else ct.time{t.convert(v)})} 
      wt.time {
        writer.addRow(list)
      }
    })
    reader.close
    writer.close
  }
  def getBoundNames(builder: CQLSSTableWriter.Builder): java.util.List[ColumnSpecification] = {
    val builderClass = classOf[CQLSSTableWriter.Builder]
    val field = builderClass.getDeclaredField("boundNames")
    field.setAccessible(true)
    val boundNames = field.get(builder).asInstanceOf[java.util.List[ColumnSpecification]]
    /*boundNames match {
      case specs: List[ColumnSpecification] => specs.map(spec => println(spec))
      case others => println(others)
    }*/
   boundNames
  }
}

