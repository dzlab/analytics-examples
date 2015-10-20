package druid

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


trait Filter extends JSONable {
}

case class NoFilter() extends Filter {
  def toJson = JObject()
}

case class SelectorFilter(val dimension: String, val value: String) extends Filter{
  def toJson = JObject(
    "type" -> "selector",
    "dimension" -> dimension, 
    "value" -> value 
    )
}

case class RegexFilter(val dimension: String, val pattern: String) extends Filter {
  def toJson = JObject(
    "type" -> "regex",
    "dimension" -> dimension, 
    "pattern" -> pattern 
    )
}

case class LogicalFilter(val typeName: String, val fields: List[Filter]) extends Filter {
  def toJson = JObject(
    "type" -> typeName,
    "fields" -> fields.map(_.toJson)
    )
}

case class NotFilter(val field: Filter) extends Filter {
  def toJson = JObject(
    "type" -> "not",
    "field" -> field.toJson
    )
}

case class JavascriptFilter(val dimension: String, val function: String) extends Filter {
  def toJson = JObject(
    "type" -> "javascript",
    "dimension" -> dimension,
    "function" -> function
    )
}
