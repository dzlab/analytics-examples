package common 

import com.github.tototoshi.csv.{CSVReader, TSVFormat}
import java.io.File
import nl.grons.metrics.scala._
import org.rogach.scallop._


object tsv extends TSVFormat {}

class Reader(input: String) {
  val reader: CSVReader = CSVReader.open(new File(input))(tsv)
  def consume(size: Int, processor: Seq[Seq[String]] => Unit) = reader.iterator.sliding(size, size).foreach(processor)
  def consume(processor: Seq[String] => Unit) = reader.iterator.foreach(processor)
}

class Writer(output: String) {
  def produce(processor: Seq[String] => Unit) = ???
}

object Global {
  /** The application wide metrics registry. */
  val metricRegistry = new com.codahale.metrics.MetricRegistry()
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Global.metricRegistry
}

class Options(args: Array[String]) extends ScallopConf(args) {
  val i = opt[String](required = false, descr = "Path to auctions (i.e. tandard_feed) file")
  val s = opt[Int](required = false, descr = "Batch size")
  val o = opt[String](required = false, descr = "Output directory where Cassandra tables will be stored")
  val n = opt[String](required = false, descr = "Node name/address")
  val p = opt[Int](required = false, descr = "Node port number")
  val r = opt[Boolean](required = false, descr = "Do any required registration/initialization")
  val c = opt[String](required = false, descr = "Path to configuration file")
  val b = opt[Boolean](required = false, descr = "Whether to use bulk upload or no")
}

