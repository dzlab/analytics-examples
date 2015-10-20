package druid 

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class Aggregation(val typeName: String, val outputName: String, val metricName: String) {
  def toJson = JObject(
        "type" -> typeName,
        "name" -> outputName,
        "fieldName" -> metricName
      )
}

trait PostAggregation{
  def toJson: JValue
}

case class ArithmeticPostAggrega(val name: String, val fn: String, val fields: List[PostAggregation], val ordering: String="null") extends PostAggregation {
  def toJson = JObject(
    "type" -> "arithmetic",
    "name" -> name,
    "fn" -> fn,
    "fields" -> fields.map(_.toJson),
    "ordering" -> ordering
    )
}

case class AccessorPostAggrega(val name: String, val fieldName: String) extends PostAggregation {
  def toJson = JObject(
    "type" -> "fieldAccess",
    "name" -> name,
    "fieldName" -> fieldName
    )
}

case class ConstantPostAggrega(val name: String, val value: String) extends PostAggregation {
  def toJson = JObject(
    "type" -> "constant",
    "name" -> name,
    "value" -> value
    )
}

case class JavascriptPostAggrega(val name: String, val fieldNames: List[String], val function: String) extends PostAggregation {
  def toJson = JObject(
    "type" -> "javascript",
    "name" -> name,
    "fieldNames" -> fieldNames,
    "function" -> function
    )
}

case class CardinalityPostAggrega(val name: String, val fieldName: String) extends PostAggregation {
  def toJson = JObject(
    "type" -> "hyperUniqueCardinality",
    "name" -> name,
    "fieldName" -> fieldName
    )
}
