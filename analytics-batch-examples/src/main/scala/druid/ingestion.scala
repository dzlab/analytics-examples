package druid 

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
 * see http://druid.io/docs/0.8.0/ingestion/index.html
 * */
case class DataSchema(val dataSource: String, val parser: Parser, val metricsSpec: List[Aggregation], val granularitySpec: GranularitySpec) {
  def toJson = JObject(
    "dataSource" -> dataSource,
    "parser" -> parser.toJson, 
    "metricsSpec" -> (for {ms <- metricsSpec } yield ms.toJson),
    "granularitySpec" -> granularitySpec.toJson 
    )
}

case class Parser(val typeName: String, val parseSpec: ParseSpec){
  def toJson = JObject(
    "type" -> typeName,
    "parseSpec" -> parseSpec.toJson
    )
}

trait ParseSpec extends JSONable {
  def format: String
  def timestampSpec: TimestampSpec
  def dimensionsSpec: DimensionsSpec
}

case class JsonParseSpec(val format: String, val timestampSpec: TimestampSpec, val dimensionsSpec: DimensionsSpec) extends ParseSpec {
  def toJson = JObject(
    "format" -> format,
    "timestampSpec" -> timestampSpec.toJson,
    "dimensionsSpec" -> dimensionsSpec.toJson
    )
}

case class TSVParseSpec(val format: String, val timestampSpec: TimestampSpec, val dimensionsSpec: DimensionsSpec, val delimiter: Option[String], val listDelimiter: Option[String], val columns: List[String]) extends ParseSpec {
  def toJson = {
    var fields: List[(String, JValue)] = List(
      "format" -> format,
      "timestampSpec" -> timestampSpec.toJson,
      "dimensionsSpec" -> dimensionsSpec.toJson,
      "columns" -> columns
    )
    delimiter match {
      case Some(d) => fields = fields :+ ("delimiter", JString(d))
      case None => fields  = fields :+ ("delimiter", JString("\t"))
    }
    listDelimiter match {
      case Some(ld) => fields = fields :+ ("listDelimiter", JString(ld))
      case None => fields
    }
    JObject(fields)
  }
}

case class TimestampSpec(val column: String, val format: String) {
  def toJson = JObject(
    "column" -> column,
    "format" -> format
    )
}

case class DimensionsSpec(val dimensions: List[String], val dimensionExclusions: List[String], val spatialDimensions: List[String]) {
  def toJson = JObject(
    "dimensions" -> dimensions,
    "dimensionExclusions" -> dimensionExclusions,
    "spatialDimensions" -> spatialDimensions
    )
}

case class GranularitySpec(val typeName: String, val segmentGranularity: String, val queryGranularity: String, val intervals: List[String]) {
  def toJson = JObject(
    "type" -> typeName,
    "segmentGranularity" -> segmentGranularity,
    "queryGranularity" -> queryGranularity,
    "intervals" -> intervals 
    )
}

case class IOConfig(val typeName: String, val firehoseType: String, val baseDir: String, val filter: String) {
  def toJson = JObject(
    "type" -> typeName,
    "firehose" -> JObject(
      "type" -> firehoseType,
      "baseDir" -> baseDir,
      "filter" -> filter
      )
    )
}

case class TuningConfig(val typeName: String, val targetPartitionSize: Int, val rowFlushBoundary: Int) {
  def toJson = JObject(
    "type" -> typeName,
    "targetPartitionSize" -> targetPartitionSize,
    "rowFlushBoundary" -> rowFlushBoundary
    )
}

case class IndexTask(val typeName: String, val dataSchema: DataSchema, val ioConfig: IOConfig, val tuningConfig: TuningConfig){
  def toJson = JObject(
    "type" -> typeName,
    "spec" -> JObject(
      "dataSchema" -> dataSchema.toJson,
      "ioConfig" -> ioConfig.toJson,
      "tuningConfig" -> tuningConfig.toJson
      )
    )
}


